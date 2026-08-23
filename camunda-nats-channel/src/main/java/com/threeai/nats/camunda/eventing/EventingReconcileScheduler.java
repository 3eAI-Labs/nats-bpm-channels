package com.threeai.nats.camunda.eventing;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Single-flight pass driver (docs/12 F-2, Flowable's fixed-delay precedent): one thread, so
 * passes are serialized by construction; the period is fixed-DELAY (a slow pass never stacks).
 *
 * <p><b>Nudge coalescing:</b> a post-commit nudge never starts a pass inline (it runs on an
 * engine transaction callback thread). It sets the dirty flag and queues a drain task; the
 * drain consumes the flag with a CAS, so N nudges arriving while a pass runs collapse into
 * exactly one follow-up pass and the flag can never be lost between the check and the run.
 *
 * <p><b>Late-binding self-check (docs/12 D-E v2):</b> deployment-driven bindings cannot be in
 * the boot-time topology self-check (they are unknown until the first pass); after the first
 * pass this logs the eventing binding summary as the late complement to that report.
 */
public class EventingReconcileScheduler implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(EventingReconcileScheduler.class);

    private final EventingReconciler reconciler;
    private final EventingRegistry registry;
    private final long periodSeconds;
    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "eventing-reconciler");
        t.setDaemon(true);
        return t;
    });
    private final AtomicBoolean dirty = new AtomicBoolean();
    private final AtomicBoolean firstPassReported = new AtomicBoolean();

    public EventingReconcileScheduler(EventingReconciler reconciler, EventingRegistry registry,
            long periodSeconds) {
        this.reconciler = reconciler;
        this.registry = registry;
        this.periodSeconds = periodSeconds;
    }

    /** Boot pass + fixed-delay period. Call once, after the engine is available. */
    public void start() {
        executor.scheduleWithFixedDelay(this::runGuarded, 0, periodSeconds, TimeUnit.SECONDS);
    }

    /** Post-commit nudge (F-2): sets the flag and queues a drain; never runs a pass inline. */
    public void nudge() {
        dirty.set(true);
        executor.execute(this::drainIfDirty);
    }

    private void drainIfDirty() {
        if (dirty.compareAndSet(true, false)) {
            runGuarded();
        }
    }

    private void runGuarded() {
        try {
            dirty.set(false); // this pass will see any state the flag pointed at
            reconciler.runPass();
            if (firstPassReported.compareAndSet(false, true)) {
                log.info("Eventing late-binding self-check: {} definition-driven subscription(s)"
                        + " active after first reconcile pass", registry.activeKeys().size());
            }
        } catch (Exception e) {
            log.error("Eventing reconcile pass failed — next pass continues on schedule", e);
        }
    }

    @Override
    public void close() {
        executor.shutdownNow();
        registry.close();
    }
}
