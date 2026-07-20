package com.threeai.nats.camunda.history;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import org.junit.jupiter.api.Test;

class HistoryOutboxRelaySchedulerTest {

    @Test
    void afterPropertiesSet_invokesRelayCyclePeriodically() throws Exception {
        HistoryOutboxRelay relay = mock(HistoryOutboxRelay.class);
        HistoryOutboxRelayScheduler scheduler = new HistoryOutboxRelayScheduler(relay, 1, "camunda");

        scheduler.afterPropertiesSet();
        try {
            verify(relay, timeout(3000).atLeast(2)).relayCycle();
        } finally {
            scheduler.destroy();
        }
    }

    @Test
    void relayCycleException_doesNotKillScheduler_retriesNextCycle() throws Exception {
        HistoryOutboxRelay relay = mock(HistoryOutboxRelay.class);
        org.mockito.Mockito.doThrow(new RuntimeException("boom")).when(relay).relayCycle();
        HistoryOutboxRelayScheduler scheduler = new HistoryOutboxRelayScheduler(relay, 1, "camunda");

        scheduler.afterPropertiesSet();
        try {
            verify(relay, timeout(3000).atLeast(2)).relayCycle();
        } finally {
            scheduler.destroy();
        }
    }

    @Test
    void destroy_stopsFurtherInvocations() throws Exception {
        HistoryOutboxRelay relay = mock(HistoryOutboxRelay.class);
        HistoryOutboxRelayScheduler scheduler = new HistoryOutboxRelayScheduler(relay, 1, "camunda");

        scheduler.afterPropertiesSet();
        verify(relay, timeout(3000).atLeast(1)).relayCycle();
        scheduler.destroy();

        int countAtDestroy = org.mockito.Mockito.mockingDetails(relay).getInvocations().size();
        Thread.sleep(1500);
        int countAfterWait = org.mockito.Mockito.mockingDetails(relay).getInvocations().size();

        assertThat(countAfterWait).isEqualTo(countAtDestroy);
    }
}
