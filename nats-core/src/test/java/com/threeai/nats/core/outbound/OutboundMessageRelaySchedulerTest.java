package com.threeai.nats.core.outbound;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

/**
 * Drives the REAL {@link java.util.concurrent.ScheduledExecutorService} the scheduler starts
 * (not a mocked clock/executor) — proves the daemon thread actually fires {@code relayCycle()}
 * repeatedly, that an uncaught exception from one cycle does not kill the schedule (next cycle
 * still runs), and that {@code destroy()} genuinely stops it.
 */
class OutboundMessageRelaySchedulerTest {

    @Test
    void afterPropertiesSet_invokesRelayCycleRepeatedly() throws Exception {
        OutboundMessageRelay relay = mock(OutboundMessageRelay.class);
        AtomicInteger invocationCount = new AtomicInteger();
        doAnswer(inv -> {
            invocationCount.incrementAndGet();
            return null;
        }).when(relay).relayCycle();

        OutboundMessageRelayScheduler scheduler = new OutboundMessageRelayScheduler(relay, 1, "camunda");
        try {
            scheduler.afterPropertiesSet();

            await().atMost(Duration.ofSeconds(5))
                    .untilAsserted(() -> verify(relay, times(2)).relayCycle());
        } finally {
            scheduler.destroy();
        }
    }

    @Test
    void runCycleSafely_relayCycleThrows_uncaughtExceptionDoesNotKillScheduleLoop() throws Exception {
        OutboundMessageRelay relay = mock(OutboundMessageRelay.class);
        doThrow(new RuntimeException("simulated cycle failure")).when(relay).relayCycle();

        OutboundMessageRelayScheduler scheduler = new OutboundMessageRelayScheduler(relay, 1, "camunda");
        try {
            scheduler.afterPropertiesSet();

            // If the uncaught exception killed the ScheduledExecutorService's periodic task, a
            // second invocation would never happen -- this is the real regression this class
            // guards against (Executors.scheduleWithFixedDelay silently stops on an uncaught
            // exception UNLESS the Runnable itself catches it, which runCycleSafely() does).
            await().atMost(Duration.ofSeconds(5))
                    .untilAsserted(() -> verify(relay, times(2)).relayCycle());
        } finally {
            scheduler.destroy();
        }
    }

    @Test
    void destroy_stopsFurtherInvocations() throws Exception {
        OutboundMessageRelay relay = mock(OutboundMessageRelay.class);
        AtomicInteger invocationCount = new AtomicInteger();
        doAnswer(inv -> {
            invocationCount.incrementAndGet();
            return null;
        }).when(relay).relayCycle();

        OutboundMessageRelayScheduler scheduler = new OutboundMessageRelayScheduler(relay, 1, "camunda");
        scheduler.afterPropertiesSet();
        await().atMost(Duration.ofSeconds(5)).until(() -> invocationCount.get() >= 1);

        scheduler.destroy();
        int countAtDestroy = invocationCount.get();
        Thread.sleep(1500); // longer than the 1s period -- would fire again if not truly stopped

        assertThat(invocationCount.get()).isEqualTo(countAtDestroy);
    }

    @Test
    void destroy_beforeAfterPropertiesSet_isNoOp_doesNotThrow() {
        OutboundMessageRelay relay = mock(OutboundMessageRelay.class);
        OutboundMessageRelayScheduler scheduler = new OutboundMessageRelayScheduler(relay, 30, "camunda");

        assertThatCode(scheduler::destroy).doesNotThrowAnyException();
    }
}
