package com.threeai.nats.history.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import javax.sql.DataSource;

import com.threeai.nats.core.vault.PseudonymizationVaultClient;
import com.threeai.nats.history.cutover.CutoverControlPlane;
import com.threeai.nats.history.cutover.CutoverRollback;
import com.threeai.nats.history.cutover.ReconciliationJob;
import com.threeai.nats.history.governance.ErasurePipeline;
import com.threeai.nats.history.governance.RetentionEnforcementJob;
import com.threeai.nats.history.projection.HistoryDlqConsumer;
import com.threeai.nats.history.projection.HistoryDlqInspectionConsumer;
import com.threeai.nats.history.projection.HistoryDlqInspectionSubscriptionRegistrar;
import com.threeai.nats.history.projection.HistoryProjectionConsumerBootstrap;
import com.threeai.nats.history.projection.ProjectionStore;
import com.threeai.nats.history.query.HistoryQueryApi;
import com.threeai.nats.history.query.HistoryQueryAuthzSpi;
import com.threeai.nats.history.query.HistoryQueryController;
import com.threeai.nats.history.query.PiiMaskingService;
import com.threeai.nats.history.query.QueryContext;
import com.threeai.nats.history.query.QueryOperation;
import io.nats.client.Connection;
import io.nats.client.JetStream;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

/**
 * Mirrors {@code CamundaNatsAutoConfigurationTest}'s discipline: bare mocks for {@code
 * Connection}/{@code JetStream} never trigger real NATS/KV I/O during bean creation (only
 * real-{@code JetStreamKvManager} calls against a REAL connection would), so these scenarios stay
 * pure unit tests despite the module's heavy conditional-wiring surface.
 */
class NatsHistoryProjectionAutoConfigurationTest {

    private static final HistoryQueryAuthzSpi ALLOW_ALL = new HistoryQueryAuthzSpi() {
        public boolean isAuthorized(QueryContext ctx, QueryOperation operation) {
            return true;
        }

        public boolean hasPiiViewPermission(QueryContext ctx) {
            return true;
        }
    };

    private final ApplicationContextRunner baseRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(NatsHistoryProjectionAutoConfiguration.class))
            .withBean(Connection.class, () -> mock(Connection.class))
            .withBean(JetStream.class, () -> mock(JetStream.class));

    @Test
    void noDataSource_registersOnlyDataSourceFreeBeans() {
        baseRunner.run(context -> {
            assertThat(context).hasSingleBean(HistoryDlqConsumer.class);
            assertThat(context).hasSingleBean(HistoryDlqInspectionConsumer.class);
            assertThat(context).hasSingleBean(HistoryDlqInspectionSubscriptionRegistrar.class);
            assertThat(context).hasSingleBean(PiiMaskingService.class);

            assertThat(context).doesNotHaveBean(ProjectionStore.class);
            assertThat(context).doesNotHaveBean(HistoryProjectionConsumerBootstrap.class);
            assertThat(context).doesNotHaveBean(HistoryQueryApi.class);
            assertThat(context).doesNotHaveBean(ErasurePipeline.class);
            assertThat(context).doesNotHaveBean(ReconciliationJob.class);
            assertThat(context).doesNotHaveBean(PseudonymizationVaultClient.class);
        });
    }

    @Test
    void projectionDataSourceOnly_registersProjectionSideButNotQueryOrEngineBoundJobs() {
        baseRunner
                .withBean("projectionDataSource", DataSource.class, () -> mock(DataSource.class))
                .run(context -> {
                    assertThat(context).hasSingleBean(ProjectionStore.class);
                    assertThat(context).hasSingleBean(HistoryProjectionConsumerBootstrap.class);
                    assertThat(context).hasSingleBean(CutoverControlPlane.class);
                    assertThat(context).hasSingleBean(CutoverRollback.class);
                    assertThat(context).hasSingleBean(RetentionEnforcementJob.class);

                    // Fail-closed: no authz SPI supplied -> no query surface, no erasure pipeline.
                    assertThat(context).doesNotHaveBean(HistoryQueryApi.class);
                    assertThat(context).doesNotHaveBean(HistoryQueryController.class);
                    assertThat(context).doesNotHaveBean(ErasurePipeline.class);
                    // No engineDataSourceReadOnly -> no reconciliation job.
                    assertThat(context).doesNotHaveBean(ReconciliationJob.class);
                    // No vaultDataSource -> no vault client.
                    assertThat(context).doesNotHaveBean(PseudonymizationVaultClient.class);
                });
    }

    @Test
    void fullWiring_projectionVaultAuthzAndEngineDataSource_registersEntireGraph() {
        baseRunner
                .withBean("projectionDataSource", DataSource.class, () -> mock(DataSource.class))
                .withBean("vaultDataSource", DataSource.class, () -> mock(DataSource.class))
                .withBean("engineDataSourceReadOnly", DataSource.class, () -> mock(DataSource.class))
                .withBean(HistoryQueryAuthzSpi.class, () -> ALLOW_ALL)
                .withPropertyValues("history.query.standalone-rest-enabled=true")
                .run(context -> {
                    assertThat(context).hasSingleBean(HistoryQueryApi.class);
                    assertThat(context).hasSingleBean(HistoryQueryController.class);
                    assertThat(context).hasSingleBean(ErasurePipeline.class);
                    assertThat(context).hasSingleBean(ReconciliationJob.class);
                    assertThat(context).hasSingleBean(PseudonymizationVaultClient.class);
                });
    }
}
