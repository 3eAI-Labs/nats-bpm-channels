package com.threeai.nats.core.jetstream;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import io.nats.client.JetStream;
import io.nats.client.api.PublishAck;
import io.nats.client.impl.NatsMessage;

/**
 * Bounded-in-flight async JetStream publishing (G4-P profile finding, 2026-08-25): the
 * synchronous per-message {@code publish()} makes the calling thread wait one broker ACK
 * round-trip per message — measured inside segment A (engine dispatch, 37 ms mean) of the
 * task cycle. This helper frees the caller from the ACK wait while keeping THREE properties
 * the sync path had:
 *
 * <ol>
 *   <li><b>Bounded memory:</b> at most {@code maxInFlight} unacknowledged publishes; the
 *       caller BLOCKS on the semaphore beyond that — under a slow broker the behavior
 *       degrades gracefully back toward synchronous, never toward unbounded future pileup.</li>
 *   <li><b>Same failure surface:</b> a failed publish invokes the caller's failure callback
 *       (same WARN + counter the sync catch produced). For A2 this changes NOTHING in the
 *       contract — a lost publish was always tolerated; the orphan sweep collects it.</li>
 *   <li><b>Dedup preserved:</b> the message's {@code Nats-Msg-Id} rides unchanged; a
 *       duplicate ACK completes normally.</li>
 * </ol>
 *
 * <p>Shutdown is best-effort by design: {@link #close} waits briefly for in-flight ACKs and
 * then abandons the wait — the callers using this class all sit on at-least-once recovery
 * paths (sweep/relay), so an ACK lost at shutdown is re-covered there.
 */
public class BoundedAsyncPublisher implements AutoCloseable {

    private final JetStream jetStream;
    private final Semaphore inFlight;
    private final int maxInFlight;

    public BoundedAsyncPublisher(JetStream jetStream, int maxInFlight) {
        if (maxInFlight < 1) {
            throw new IllegalArgumentException("maxInFlight must be >= 1 — got " + maxInFlight);
        }
        this.jetStream = jetStream;
        this.maxInFlight = maxInFlight;
        this.inFlight = new Semaphore(maxInFlight);
    }

    /**
     * Publishes asynchronously; blocks only when {@code maxInFlight} ACKs are outstanding.
     * Exactly one of the callbacks fires, on a jnats internal thread — keep them cheap.
     */
    public void publish(NatsMessage msg, Runnable onAck, Consumer<Throwable> onFailure) {
        try {
            inFlight.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            onFailure.accept(e);
            return;
        }
        CompletableFuture<PublishAck> future;
        try {
            future = jetStream.publishAsync(msg);
        } catch (RuntimeException e) {
            inFlight.release();
            onFailure.accept(e);
            return;
        }
        future.whenComplete((ack, error) -> {
            inFlight.release();
            if (error != null) {
                onFailure.accept(error);
            } else {
                onAck.run();
            }
        });
    }

    /** Best-effort drain: waits up to {@code millis} for outstanding ACKs, then gives up. */
    public boolean awaitQuiescence(long millis) {
        try {
            if (inFlight.tryAcquire(maxInFlight, millis, TimeUnit.MILLISECONDS)) {
                inFlight.release(maxInFlight);
                return true;
            }
            return false;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    @Override
    public void close() {
        awaitQuiescence(2000);
    }
}
