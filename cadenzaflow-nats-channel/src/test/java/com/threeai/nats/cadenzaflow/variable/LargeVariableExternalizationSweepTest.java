package com.threeai.nats.cadenzaflow.variable;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.UUID;
import javax.sql.DataSource;

import com.threeai.nats.core.jetstream.SweepLeaderLease;
import com.threeai.nats.core.largepayload.ContentAddressedLargePayloadStore;
import com.threeai.nats.core.largepayload.ContentHash;
import com.threeai.nats.core.largepayload.LargeVariableExternalizationProperties;
import com.threeai.nats.core.largepayload.RuntimeVariableReference;
import org.junit.jupiter.api.Test;

/** Leader-gating/kill-switch + reconciliation-ledger-read branch coverage — the real-SQL-against-
 *  real-Postgres candidate discovery AND reconciliation paths (both the stale-release and the
 *  shared-reference-survives cases) are exercised by {@link LargeVariableExternalizationE2eTest}. */
class LargeVariableExternalizationSweepTest {

    @Test
    void sweepCycle_notLeader_zeroWork_neverExternalizes() {
        DataSource dataSource = mock(DataSource.class);
        SweepLeaderLease leaderLease = mock(SweepLeaderLease.class);
        LargeVariablePostCommitExternalizer externalizer = mock(LargeVariablePostCommitExternalizer.class);
        ContentAddressedLargePayloadStore payloadStore = mock(ContentAddressedLargePayloadStore.class);
        LargeVariableExternalizationProperties properties = new LargeVariableExternalizationProperties();
        when(leaderLease.tryAcquireOrRenew()).thenReturn(false);
        LargeVariableExternalizationSweep sweep = new LargeVariableExternalizationSweep(
                dataSource, leaderLease, externalizer, payloadStore, properties, "cadenzaflow");

        sweep.sweepCycle();

        verify(externalizer, never()).externalizeNow(any());
        verify(payloadStore, never()).listRuntimeReferences(any()); // reconciliation never even attempted
    }

    @Test
    void sweepCycle_disabled_zeroWork_leaseNeverEvenChecked() {
        DataSource dataSource = mock(DataSource.class);
        SweepLeaderLease leaderLease = mock(SweepLeaderLease.class);
        LargeVariablePostCommitExternalizer externalizer = mock(LargeVariablePostCommitExternalizer.class);
        ContentAddressedLargePayloadStore payloadStore = mock(ContentAddressedLargePayloadStore.class);
        LargeVariableExternalizationProperties properties = new LargeVariableExternalizationProperties();
        properties.setEnabled(false);
        LargeVariableExternalizationSweep sweep = new LargeVariableExternalizationSweep(
                dataSource, leaderLease, externalizer, payloadStore, properties, "cadenzaflow");

        sweep.sweepCycle();

        verify(leaderLease, never()).tryAcquireOrRenew();
        verify(externalizer, never()).externalizeNow(any());
    }

    @Test
    void reconcileRuntimeReferences_emptyLedger_noopReleaseNeverCalled() {
        DataSource dataSource = mock(DataSource.class);
        SweepLeaderLease leaderLease = mock(SweepLeaderLease.class);
        LargeVariablePostCommitExternalizer externalizer = mock(LargeVariablePostCommitExternalizer.class);
        ContentAddressedLargePayloadStore payloadStore = mock(ContentAddressedLargePayloadStore.class);
        LargeVariableExternalizationProperties properties = new LargeVariableExternalizationProperties();
        when(payloadStore.listRuntimeReferences("cadenzaflow")).thenReturn(List.of());
        LargeVariableExternalizationSweep sweep = new LargeVariableExternalizationSweep(
                dataSource, leaderLease, externalizer, payloadStore, properties, "cadenzaflow");

        sweep.reconcileRuntimeReferences();

        verify(payloadStore, never()).releaseReference(any());
    }

    @Test
    void reconcileRuntimeReferences_ledgerReadFails_doesNotThrow() {
        DataSource dataSource = mock(DataSource.class);
        SweepLeaderLease leaderLease = mock(SweepLeaderLease.class);
        LargeVariablePostCommitExternalizer externalizer = mock(LargeVariablePostCommitExternalizer.class);
        ContentAddressedLargePayloadStore payloadStore = mock(ContentAddressedLargePayloadStore.class);
        LargeVariableExternalizationProperties properties = new LargeVariableExternalizationProperties();
        when(payloadStore.listRuntimeReferences("cadenzaflow")).thenThrow(new RuntimeException("projection DB down"));
        LargeVariableExternalizationSweep sweep = new LargeVariableExternalizationSweep(
                dataSource, leaderLease, externalizer, payloadStore, properties, "cadenzaflow");

        assertThatCode(sweep::reconcileRuntimeReferences).doesNotThrowAnyException();
    }

    @Test
    void reconcileRuntimeReferences_engineDataSourceUnreachable_releaseNotAttempted_doesNotThrow() throws Exception {
        // Ledger has a row, but the engine DataSource (used to batch-read CURRENT markers) is
        // unreachable -- must fail closed (no release without genuinely comparing against live
        // state), not accidentally release a still-live reference.
        DataSource unreachableEngineDataSource = mock(DataSource.class);
        SweepLeaderLease leaderLease = mock(SweepLeaderLease.class);
        LargeVariablePostCommitExternalizer externalizer = mock(LargeVariablePostCommitExternalizer.class);
        ContentAddressedLargePayloadStore payloadStore = mock(ContentAddressedLargePayloadStore.class);
        LargeVariableExternalizationProperties properties = new LargeVariableExternalizationProperties();
        String hash = ContentHash.sha256Hex("x".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        when(payloadStore.listRuntimeReferences("cadenzaflow")).thenReturn(
                List.of(new RuntimeVariableReference("cadenzaflow", "var-1", UUID.randomUUID(), hash)));
        when(unreachableEngineDataSource.getConnection()).thenThrow(new java.sql.SQLException("connection refused"));
        LargeVariableExternalizationSweep sweep = new LargeVariableExternalizationSweep(
                unreachableEngineDataSource, leaderLease, externalizer, payloadStore, properties, "cadenzaflow");

        assertThatCode(sweep::reconcileRuntimeReferences).doesNotThrowAnyException();

        verify(payloadStore, never()).releaseReference(any());
    }
}
