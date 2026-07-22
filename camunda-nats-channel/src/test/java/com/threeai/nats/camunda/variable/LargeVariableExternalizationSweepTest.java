package com.threeai.nats.camunda.variable;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import javax.sql.DataSource;

import com.threeai.nats.core.jetstream.SweepLeaderLease;
import com.threeai.nats.core.largepayload.LargeVariableExternalizationProperties;
import org.junit.jupiter.api.Test;

/** Leader-gating/kill-switch branch coverage — the real-SQL-against-real-Postgres candidate
 *  discovery path is exercised by {@link LargeVariableExternalizationE2eTest#sweepCycle_findsAndExternalizesVariable_fastPathDidNotHandle}. */
class LargeVariableExternalizationSweepTest {

    @Test
    void sweepCycle_notLeader_zeroWork_neverExternalizes() {
        DataSource dataSource = mock(DataSource.class);
        SweepLeaderLease leaderLease = mock(SweepLeaderLease.class);
        LargeVariablePostCommitExternalizer externalizer = mock(LargeVariablePostCommitExternalizer.class);
        LargeVariableExternalizationProperties properties = new LargeVariableExternalizationProperties();
        when(leaderLease.tryAcquireOrRenew()).thenReturn(false);
        LargeVariableExternalizationSweep sweep =
                new LargeVariableExternalizationSweep(dataSource, leaderLease, externalizer, properties, "camunda");

        sweep.sweepCycle();

        verify(externalizer, never()).externalizeNow(any());
    }

    @Test
    void sweepCycle_disabled_zeroWork_leaseNeverEvenChecked() {
        DataSource dataSource = mock(DataSource.class);
        SweepLeaderLease leaderLease = mock(SweepLeaderLease.class);
        LargeVariablePostCommitExternalizer externalizer = mock(LargeVariablePostCommitExternalizer.class);
        LargeVariableExternalizationProperties properties = new LargeVariableExternalizationProperties();
        properties.setEnabled(false);
        LargeVariableExternalizationSweep sweep =
                new LargeVariableExternalizationSweep(dataSource, leaderLease, externalizer, properties, "camunda");

        sweep.sweepCycle();

        verify(leaderLease, never()).tryAcquireOrRenew();
        verify(externalizer, never()).externalizeNow(any());
    }
}
