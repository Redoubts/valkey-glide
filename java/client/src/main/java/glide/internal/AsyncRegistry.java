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
 * <p>Timeouts are enforced by a 1ms periodic sweep that checks stored deadlines, replacing
 * per-request {@code ScheduledFuture} allocation. Cleanup is performed via atomic {@code remove()}
 * in each completion path, avoiding {@code CompletableFuture.whenComplete()} overhead.
 */
public final class AsyncRegistry {

    private static final int SWEEP_INTERVAL_MS = 1;

    private static final class Entry {
        final CompletableFuture<Object> future;
        final long deadlineMs;
        final int maxInflightRequests;
        final long clientHandle;

        Entry(
                CompletableFuture<Object> future,
                long deadlineMs,
                int maxInflightRequests,
                long clientHandle) {
            this.future = future;
            this.deadlineMs = deadlineMs;
            this.maxInflightRequests = maxInflightRequests;
            this.clientHandle = clientHandle;
        }
    }

    private static final ConcurrentHashMap<Long, Entry> activeFutures =
            new ConcurrentHashMap<>(estimateInitialCapacity());

    private static final ConcurrentHashMap<Long, AtomicInteger> clientInflightCounts =
            new ConcurrentHashMap<>();

    private static final AtomicLong nextId = new AtomicLong(1);

    private static final AtomicBoolean isShutdown = new AtomicBoolean(false);

    private static final ScheduledExecutorService sweepScheduler =
            Executors.newSingleThreadScheduledExecutor(
                    r -> {
                        Thread t = new Thread(r, "GlideTimeoutSweep");
                        t.setDaemon(true);
                        return t;
                    });

    private static volatile ScheduledFuture<?> sweepTask;

    private static final Thread shutdownHook =
            new Thread(AsyncRegistry::shutdown, "AsyncRegistry-Shutdown");

    static {
        if (!"false".equalsIgnoreCase(System.getProperty("glide.autoShutdownHook", "true"))) {
            Runtime.getRuntime().addShutdownHook(shutdownHook);
        }
    }

    private static int estimateInitialCapacity() {
        for (String source :
                new String[] {
                    System.getenv("GLIDE_MAX_INFLIGHT_REQUESTS"),
                    System.getProperty("glide.maxInflightRequests")
                }) {
            if (source != null) {
                try {
                    int v = Integer.parseInt(source.trim());
                    if (v > 0) return Math.max(16, v * 2);
                } catch (NumberFormatException ignored) {
                }
            }
        }
        return 2000;
    }

    // ---- Sweep ----

    private static void ensureSweepRunning() {
        if (sweepTask == null) {
            synchronized (AsyncRegistry.class) {
                if (sweepTask == null) {
                    sweepTask =
                            sweepScheduler.scheduleAtFixedRate(
                                    AsyncRegistry::sweepTimedOut,
                                    SWEEP_INTERVAL_MS,
                                    SWEEP_INTERVAL_MS,
                                    TimeUnit.MILLISECONDS);
                }
            }
        }
    }

    private static void sweepTimedOut() {
        long now = System.currentTimeMillis();
        activeFutures.forEach(
                (id, entry) -> {
                    if (entry.deadlineMs <= now) {
                        Entry removed = activeFutures.remove(id);
                        if (removed != null
                                && removed.future.completeExceptionally(
                                        new TimeoutException("Request timed out"))) {
                            releaseInflight(removed);
                            GlideNativeBridge.markTimedOut(id);
                        }
                    }
                });
    }

    // ---- Registration ----

    /**
     * Register a future for native callback correlation with optional timeout and inflight limit.
     *
     * @return correlation ID, or 0 if the registry is shutting down
     */
    public static <T> long register(
            CompletableFuture<T> future, int maxInflightRequests, long clientHandle, long timeoutMillis) {
        if (future == null) {
            throw new IllegalArgumentException("Future cannot be null");
        }

        if (isShutdown.get()) {
            future.completeExceptionally(
                    new ClosingException("Client is shutting down, cannot register new requests"));
            return 0L;
        }

        if (maxInflightRequests > 0) {
            enforceInflightLimit(clientHandle, maxInflightRequests);
        }

        long correlationId = nextId.getAndIncrement();

        @SuppressWarnings("unchecked")
        CompletableFuture<Object> originalFuture = (CompletableFuture<Object>) future;

        long deadline = timeoutMillis > 0 ? System.currentTimeMillis() + timeoutMillis : Long.MAX_VALUE;

        activeFutures.put(
                correlationId, new Entry(originalFuture, deadline, maxInflightRequests, clientHandle));

        if (isShutdown.get()) {
            Entry removed = activeFutures.remove(correlationId);
            if (removed != null) {
                releaseInflight(removed);
            }
            future.completeExceptionally(
                    new ClosingException("Client is shutting down, cannot register new requests"));
            return 0L;
        }

        if (timeoutMillis > 0) {
            ensureSweepRunning();
        }

        return correlationId;
    }

    // ---- Completion ----

    /** Complete with success. Returns false if already completed or timed out. */
    public static boolean completeCallback(long correlationId, Object result) {
        Entry entry = activeFutures.remove(correlationId);
        if (entry == null) {
            return false;
        }
        boolean completed = entry.future.complete(result);
        if (completed) {
            releaseInflight(entry);
        }
        return completed;
    }

    /**
     * Complete with error. Codes: 0=Unspecified, 1=ExecAbort, 2=Timeout, 3=Disconnect. Returns false
     * if already completed or timed out.
     */
    public static boolean completeCallbackWithErrorCode(
            long correlationId, int errorTypeCode, String errorMessage) {
        Entry entry = activeFutures.remove(correlationId);
        if (entry == null) {
            return false;
        }

        String msg =
                (errorMessage == null || errorMessage.trim().isEmpty())
                        ? "Unknown error from native code"
                        : errorMessage;

        RuntimeException ex;
        switch (errorTypeCode) {
            case 1:
                ex = new ExecAbortException(msg);
                break;
            case 2:
                ex = new TimeoutException(msg);
                break;
            case 3:
                ex = new ClosingException(msg);
                break;
            default:
                ex = new RequestException(msg);
                break;
        }

        boolean completed = entry.future.completeExceptionally(ex);
        if (completed) {
            releaseInflight(entry);
        }
        return completed;
    }

    // ---- Inflight tracking ----

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

    private static void releaseInflight(Entry entry) {
        if (entry.maxInflightRequests > 0) {
            clientInflightCounts.computeIfPresent(
                    entry.clientHandle, (key, counter) -> counter.decrementAndGet() <= 0 ? null : counter);
        }
    }

    // ---- Lifecycle ----

    public static void shutdown() {
        isShutdown.set(true);
        cancelSweep();
        activeFutures.values().forEach(entry -> entry.future.cancel(true));
        activeFutures.clear();
        clientInflightCounts.clear();
        sweepScheduler.shutdownNow();
    }

    /**
     * Fail all pending futures with a {@link ClosingException}. Called from native on fatal failure.
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
        cancelSweep();
        clientInflightCounts.clear();
    }

    public static void cleanupClient(long clientHandle) {
        clientInflightCounts.remove(clientHandle);
    }

    /** Reset all state. For test isolation. */
    public static void reset() {
        isShutdown.set(false);
        cancelSweep();
        synchronized (AsyncRegistry.class) {
            sweepTask = null;
        }
        activeFutures.clear();
        clientInflightCounts.clear();
        nextId.set(1);
    }

    private static void cancelSweep() {
        ScheduledFuture<?> task = sweepTask;
        if (task != null) {
            task.cancel(false);
        }
    }

    // ---- Observability ----

    public static int getPendingCount() {
        return activeFutures.size();
    }

    public static int getActiveFutureCount() {
        return activeFutures.size();
    }

    public static boolean isShutdown() {
        return isShutdown.get();
    }

    public static void removeShutdownHook() {
        try {
            Runtime.getRuntime().removeShutdownHook(shutdownHook);
        } catch (IllegalStateException ignored) {
        }
    }
}
