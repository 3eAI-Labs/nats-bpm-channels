package com.threeai.nats.cibseven.eventing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.cibseven.bpm.engine.RepositoryService;
import org.cibseven.bpm.engine.RuntimeService;
import org.junit.jupiter.api.Test;

/** F-2: single-flight passes, nudge coalescing, boot pass. */
class EventingReconcileSchedulerTest {

    /** Counting stand-in; first pass blocks until released so nudges can pile up behind it. */
    private static class BlockingReconciler extends EventingReconciler {
        final AtomicInteger passes = new AtomicInteger();
        final CountDownLatch firstPassEntered = new CountDownLatch(1);
        final CountDownLatch releaseFirstPass = new CountDownLatch(1);

        BlockingReconciler() {
            super(mock(RepositoryService.class), mock(RuntimeService.class),
                    new EventingRegistry((config, event) -> () -> { }));
        }

        @Override
        public synchronized void runPass() {
            int n = passes.incrementAndGet();
            if (n == 1) {
                firstPassEntered.countDown();
                try {
                    releaseFirstPass.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    @Test
    void nudgesDuringRunningPass_coalesceIntoOneFollowUp() throws Exception {
        BlockingReconciler reconciler = new BlockingReconciler();
        EventingRegistry registry = new EventingRegistry((config, event) -> () -> { });
        try (EventingReconcileScheduler scheduler =
                new EventingReconcileScheduler(reconciler, registry, 3600)) {
            scheduler.start(); // boot pass enters and blocks
            assertThat(reconciler.firstPassEntered.await(5, TimeUnit.SECONDS)).isTrue();

            for (int i = 0; i < 5; i++) {
                scheduler.nudge(); // five nudges while the pass runs
            }
            reconciler.releaseFirstPass.countDown();

            // the queued drains run on the single scheduler thread right after the boot pass;
            // exactly ONE of them consumes the dirty flag (CAS) -> exactly one follow-up pass
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (reconciler.passes.get() < 2 && System.nanoTime() < deadline) {
                Thread.sleep(10);
            }
            Thread.sleep(100); // allow any (incorrect) extra drains to surface
            assertThat(reconciler.passes.get()).isEqualTo(2);
        }
    }

    @Test
    void nudgeAfterIdle_runsExactlyOnePass() throws Exception {
        BlockingReconciler reconciler = new BlockingReconciler();
        reconciler.releaseFirstPass.countDown(); // don't block
        EventingRegistry registry = new EventingRegistry((config, event) -> () -> { });
        try (EventingReconcileScheduler scheduler =
                new EventingReconcileScheduler(reconciler, registry, 3600)) {
            scheduler.start();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (reconciler.passes.get() < 1 && System.nanoTime() < deadline) {
                Thread.sleep(10);
            }

            scheduler.nudge();
            deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (reconciler.passes.get() < 2 && System.nanoTime() < deadline) {
                Thread.sleep(10);
            }
            Thread.sleep(100);
            assertThat(reconciler.passes.get()).isEqualTo(2);
        }
    }
}
