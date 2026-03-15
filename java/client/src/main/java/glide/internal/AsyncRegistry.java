/** Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0 */
package glide.internal;

import glide.api.models.exceptions.ClosingException;
import glide.api.models.exceptions.ExecAbortException;
import glide.api.models.exceptions.RequestException;
import glide.api.models.exceptions.TimeoutException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Async registry for correlating native callbacks with Java {@link CompletableFuture}s.
 *
 * <p>Responsibilities:
 *
 * <ul>
 *   <li>Maintain a thread-safe mapping from correlation id to the original future
 *   <li>Enforce per-client max inflight requests in Java (0 = defer to core default)
 *   <li>Enforce timeouts via a periodic sweep instead of per-request scheduled tasks
 *   <li>Perform atomic cleanup on completion to avoid races and leaks
 * </ul>
 *
 * <p>Timeouts use a periodic sweep over {@code activeFutures}. Each entry stores the future and its
 * deadline, avoiding per-request scheduling overhead. This reduces timer task creation from O(N) to
 * O(1) and eliminates the throughput regression caused by per-request ScheduledFuture allocation.
 */
public final class AsyncRegistry {

    /** Entry holding the future and its deadline for the timeout sweep. */
    private static final class Entry {
        final CompletableFuture<Object> future;
        final long deadlineMs; // System.currentTimeMillis() + timeout, or Long.MAX_VALUE for no timeout

        Entry(CompletableFuture<Object> future, long deadlineMs) {
            this.future = future;
            this.deadlineMs = deadlineMs;
        }
    }

    /** Thread-safe storage for active futures. Using ConcurrentHashMap for lock-free operations. */
    private static final ConcurrentHashMap<Long, Entry> activeFutures =
            new ConcurrentHashMap<>(estimateInitialCapacity());

    /**
     * Per-client inflight request counters. Maps client handle to the number of active requests for
     * that client.
     */
    private static final ConcurrentHashMap<Long, AtomicInteger> clientInflightCounts =
            new ConcurrentHashMap<>();

    /** Thread-safe ID generator for correlation IDs. */
    private static final AtomicLong nextId = new AtomicLong(1);

    /**
     * Shutdown flag to prevent race conditions between register() and shutdown()/failAllWithError().
     * Once set to true, register() will return pre-failed futures instead of adding to the registry.
     */
    private static final AtomicBoolean isShutdown = new AtomicBoolean(false);

    /**
     * Single-threaded scheduler for the periodic timeout sweep. Uses a daemon thread so it won't
     * prevent JVM shutdown. One fixed-rate task replaces per-request ScheduledFuture allocation.
     */
    private static final ScheduledExecutorService timeoutScheduler =
            Executors.newSingleThreadScheduledExecutor(
                    r -> {
                        Thread t = new Thread(r, "GlideTimeoutSweep");
                        t.setDaemon(true);
                        return t;
                    });

    /** Handle for the periodic sweep task, used for cancellation during shutdown. */
    private static volatile ScheduledFuture<?> sweepTask;

    private static final Thread shutdownHook =
            new Thread(AsyncRegistry::shutdown, "AsyncRegistry-Shutdown");

    static {
        if (!"false".equalsIgnoreCase(System.getProperty("glide.autoShutdownHook", "true"))) {
            Runtime.getRuntime().addShutdownHook(shutdownHook);
        }
    }

    /** Estimate initial capacity for the active futures map using inflight limit with margin. */
    private static int estimateInitialCapacity() {
        String env = System.getenv("GLIDE_MAX_INFLIGHT_REQUESTS");
        if (env != null) {
            try {
                int v = Integer.parseInt(env.trim());
                if (v > 0) return Math.max(16, v * 2);
            } catch (NumberFormatException ignored) {
            }
        }

        String prop = System.getProperty("glide.maxInflightRequests");
        if (prop != null) {
            try {
                int v = Integer.parseInt(prop.trim());
                if (v > 0) return Math.max(16, v * 2);
            } catch (NumberFormatException ignored) {
            }
        }

        return 2000; // Default with margin over core's 1000
    }

    /**
     * Start the periodic sweep if not already running. Called lazily on first timeout registration.
     */
    private static void ensureSweepRunning() {
        if (sweepTask == null) {
            synchronized (AsyncRegistry.class) {
                if (sweepTask == null) {
                    sweepTask =
                            timeoutScheduler.scheduleAtFixedRate(
                                    AsyncRegistry::sweepTimedOut, 100, 100, TimeUnit.MILLISECONDS);
                }
            }
        }
    }

    /** Sweep all entries whose deadline has passed and complete them with TimeoutException. */
    private static void sweepTimedOut() {
        long now = System.currentTimeMillis();
        activeFutures.forEach(
                (id, entry) -> {
                    if (entry.deadlineMs <= now) {
                        if (entry.future.completeExceptionally(new TimeoutException("Request timed out"))) {
                            GlideNativeBridge.markTimedOut(id);
                        }
                    }
                });
    }

    /**
     * Register future with client-specific inflight limit, client handle for per-client tracking, and
     * optional Java-side timeout.
     *
     * <p>If the registry is shutting down, the future will be completed exceptionally with a
     * ClosingException and a special correlation ID (0) will be returned to indicate the registration
     * failed.
     *
     * @param future the future to register
     * @param maxInflightRequests per-client limit (0 = no Java-side limit, defer to core)
     * @param clientHandle native client handle for tracking
     * @param timeoutMillis Java-side timeout in milliseconds (0 = use Rust default timeout)
     * @return correlation ID for native callback, or 0 if shutdown is in progress
     */
    public static <T> long register(
            CompletableFuture<T> future, int maxInflightRequests, long clientHandle, long timeoutMillis) {
        if (future == null) {
            throw new IllegalArgumentException("Future cannot be null");
        }

        // Check shutdown flag before registering to prevent race conditions
        // This ensures no futures are added after shutdown() starts clearing
        if (isShutdown.get()) {
            future.completeExceptionally(
                    new ClosingException("Client is shutting down, cannot register new requests"));
            return 0L; // Special ID indicating registration failed
        }

        // Client-specific inflight limit check
        // 0 means "use native/core defaults" - no limit enforcement in Java layer
        if (maxInflightRequests > 0) {
            enforceInflightLimit(clientHandle, maxInflightRequests);
        }

        long correlationId = nextId.getAndIncrement();

        @SuppressWarnings("unchecked")
        CompletableFuture<Object> originalFuture = (CompletableFuture<Object>) future;

        long deadline = timeoutMillis > 0 ? System.currentTimeMillis() + timeoutMillis : Long.MAX_VALUE;

        activeFutures.put(correlationId, new Entry(originalFuture, deadline));

        // Double-check shutdown flag after insertion to handle race with shutdown()
        if (isShutdown.get()) {
            activeFutures.remove(correlationId);
            if (maxInflightRequests > 0) {
                decrementInflightCount(clientHandle);
            }
            future.completeExceptionally(
                    new ClosingException("Client is shutting down, cannot register new requests"));
            return 0L;
        }

        // Ensure the sweep is running if we have a timeout to enforce
        if (timeoutMillis > 0) {
            ensureSweepRunning();
        }

        // Set up cleanup on the original future
        setupCleanup(correlationId, originalFuture, maxInflightRequests, clientHandle);

        return correlationId;
    }

    /** Enforce per-client inflight limit, throwing RequestException if exceeded. */
    private static void enforceInflightLimit(long clientHandle, int maxInflightRequests) {
        clientInflightCounts.compute(
                clientHandle,
                (key, counter) -> {
                    AtomicInteger value = counter != null ? counter : new AtomicInteger(0);
                    if (value.incrementAndGet() > maxInflightRequests) {
                        value.decrementAndGet();
                        throw new RequestException("Client reached maximum inflight requests");
                    }
                    return value;
                });
    }

    /**
     * Set up cleanup handler for when the future completes (success, error, or timeout). Performs
     * atomic cleanup to avoid races and leaks.
     */
    private static void setupCleanup(
            long correlationId,
            CompletableFuture<Object> future,
            int maxInflightRequests,
            long clientHandle) {
        future.whenComplete(
                (result, error) -> {
                    // Atomic cleanup - no race conditions
                    activeFutures.remove(correlationId);

                    // Decrement per-client counter if applicable
                    if (maxInflightRequests > 0) {
                        decrementInflightCount(clientHandle);
                    }
                });
    }

    /** Decrement inflight count for client, removing the entry when it reaches zero. */
    private static void decrementInflightCount(long clientHandle) {
        clientInflightCounts.computeIfPresent(
                clientHandle,
                (key, counter) -> {
                    int remaining = counter.decrementAndGet();
                    return remaining <= 0 ? null : counter;
                });
    }

    /**
     * Complete callback with proper race condition handling. Returns false if already completed or
     * timed out.
     */
    public static boolean completeCallback(long correlationId, Object result) {
        Entry entry = activeFutures.get(correlationId);
        return entry != null && entry.future.complete(result);
    }

    /**
     * Complete with error using a structured error code from native layer. Codes map to glide-core
     * RequestErrorType: 0=Unspecified, 1=ExecAbort, 2=Timeout, 3=Disconnect.
     */
    public static boolean completeCallbackWithErrorCode(
            long correlationId, int errorTypeCode, String errorMessage) {
        Entry entry = activeFutures.get(correlationId);
        if (entry == null) {
            return false;
        }

        String msg =
                (errorMessage == null || errorMessage.trim().isEmpty())
                        ? "Unknown error from native code"
                        : errorMessage;

        RuntimeException ex;
        switch (errorTypeCode) {
            case 2:
                ex = new TimeoutException(msg);
                break;
            case 3:
                ex = new ClosingException(msg);
                break;
            case 1:
                ex = new ExecAbortException(msg);
                break;
            default:
                ex = new RequestException(msg);
                break;
        }

        return entry.future.completeExceptionally(ex);
    }

    /** Get current pending operation count. */
    public static int getPendingCount() {
        return activeFutures.size();
    }

    /** Shutdown cleanup - cancel all pending operations during client shutdown. */
    public static void shutdown() {
        isShutdown.set(true);

        ScheduledFuture<?> task = sweepTask;
        if (task != null) {
            task.cancel(false);
        }

        activeFutures.values().forEach(entry -> entry.future.cancel(true));
        activeFutures.clear();
        clientInflightCounts.clear();

        timeoutScheduler.shutdownNow();
    }

    /**
     * Fail all pending futures with a {@link ClosingException}. Called from the native layer when a
     * fatal infrastructure failure is detected.
     */
    public static void failAllWithError(String errorMessage) {
        isShutdown.set(true);

        String msg =
                (errorMessage == null || errorMessage.isEmpty())
                        ? "Native callback infrastructure failed"
                        : errorMessage;
        activeFutures.forEach(
                (id, entry) -> entry.future.completeExceptionally(new ClosingException(msg)));
        activeFutures.clear();

        ScheduledFuture<?> task = sweepTask;
        if (task != null) {
            task.cancel(false);
        }

        clientInflightCounts.clear();
    }

    /** Clean up per-client tracking when a client is closed. */
    public static void cleanupClient(long clientHandle) {
        clientInflightCounts.remove(clientHandle);
    }

    /** Reset all internal state. Intended for test isolation and client shutdown cleanup. */
    public static void reset() {
        isShutdown.set(false);

        ScheduledFuture<?> task = sweepTask;
        if (task != null) {
            task.cancel(false);
        }
        synchronized (AsyncRegistry.class) {
            sweepTask = null;
        }

        activeFutures.clear();
        clientInflightCounts.clear();
        nextId.set(1);
    }

    /**
     * Returns the count of active futures. Intended for testing to verify futures are cleaned up
     * properly.
     */
    public static int getActiveFutureCount() {
        return activeFutures.size();
    }

    /** Returns whether the registry is in shutdown state. */
    public static boolean isShutdown() {
        return isShutdown.get();
    }

    /**
     * Remove the automatic shutdown hook, allowing users to manage shutdown manually. Call this if
     * you want to control shutdown behavior yourself.
     */
    public static void removeShutdownHook() {
        try {
            Runtime.getRuntime().removeShutdownHook(shutdownHook);
        } catch (IllegalStateException ignored) {
            // Hook was never registered or already removed
        }
    }
}
