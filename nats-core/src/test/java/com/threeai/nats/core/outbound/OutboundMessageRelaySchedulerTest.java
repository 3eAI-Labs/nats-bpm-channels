package com.threeai.nats.core.outbound;

import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import org.junit.jupiter.api.Test;

/**
 * Every periodic loop in this codebase runs under {@code scheduleWithFixedDelay}, whose contract
 * cancels the task FOREVER on any escaped throwable — silently, because the throwable is captured
 * into a {@code ScheduledFuture} nobody reads. A {@code catch (Exception)} wrapper leaves the
 * loop one {@link Error} away from silent permanent death; this was one of the two suspects the
 * 2026-08-20 flatline investigation had to rule out blind, precisely because nothing would have
 * logged. Pins the {@code catch (Throwable)} contract on the one scheduler that lives in
 * nats-core — the module schedulers (A2 sweep, history relay) are copies of the same wrapper.
 */
class OutboundMessageRelaySchedulerTest {

    @Test
    void periodicTaskSurvivesAnErrorEscapingTheCycle() {
        OutboundMessageRelay relay = mock(OutboundMessageRelay.class);
        doThrow(new OutOfMemoryError("simulated allocation failure")).when(relay).relayCycle();
        OutboundMessageRelayScheduler scheduler = new OutboundMessageRelayScheduler(relay, 1, "test-engine");

        scheduler.afterPropertiesSet();
        try {
            // With catch (Exception) this stops at exactly one invocation — the Error cancels the
            // periodic task. Two or more invocations prove the loop outlives an Error.
            verify(relay, timeout(3500).atLeast(2)).relayCycle();
        } finally {
            scheduler.destroy();
        }
    }
}
