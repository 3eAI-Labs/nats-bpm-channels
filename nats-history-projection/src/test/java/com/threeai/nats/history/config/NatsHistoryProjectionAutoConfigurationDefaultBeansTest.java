package com.threeai.nats.history.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;
import javax.sql.DataSource;

import com.threeai.nats.history.projection.HistoryDlqInspectionConsumer;
import com.threeai.nats.core.metrics.NatsChannelMetrics;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.nats.client.Connection;
import io.nats.client.JetStream;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Sentinel Phase 5.5 (round 2): {@code NatsHistoryProjectionAutoConfigurationTest} only ever
 * supplies user-provided Connection/JetStream/DataSource beans, so this auto-configuration's OWN
 * default {@code @ConditionalOnMissingBean} factory methods (natsConnection/natsJetStream/
 * natsChannelMetrics/projectionDataSource/vaultDataSource) were never actually invoked. These
 * tests exercise them directly, mirroring the same real-Testcontainers-broker /
 * real-Testcontainers-Postgres pattern used elsewhere in this module and in
 * flowable-nats-channel's analogous default-beans test.
 */
@Testcontainers
class NatsHistoryProjectionAutoConfigurationDefaultBeansTest {

    @Container
    static org.testcontainers.containers.GenericContainer<?> natsContainer =
            new org.testcontainers.containers.GenericContainer<>("nats:2.10-alpine").withExposedPorts(4222);

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16");

    @Test
    void noUserConnectionBean_createsRealConnectionViaDefaultFactoryBean() {
        String natsUrl = "nats://" + natsContainer.getHost() + ":" + natsContainer.getMappedPort(4222);
        // QA-FINDING-2 (fixed): NatsHistoryProjectionAutoConfiguration now carries its own
        // @EnableConfigurationProperties(NatsProperties.class) (mirrors FlowableNatsAutoConfiguration),
        // so natsConnection(NatsProperties props) resolves standalone -- no co-located
        // camunda/flowable-nats-channel auto-configuration and no tenant-supplied NatsProperties
        // bean needed. Previously this bean failed with NoSuchBeanDefinitionException in exactly
        // this shape (module running alone, no user Connection bean); this test proves the fix by
        // deliberately NOT supplying NatsProperties from anywhere else.
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(NatsHistoryProjectionAutoConfiguration.class))
                .withBean(JetStream.class, () -> mock(JetStream.class))
                .withPropertyValues("spring.nats.url=" + natsUrl)
                .run(context -> {
                    assertThat(context).hasSingleBean(Connection.class);
                    Connection connection = context.getBean(Connection.class);
                    assertThatCode(() -> connection.flush(Duration.ofSeconds(2))).doesNotThrowAnyException();
                    connection.close();
                });
    }

    @Test
    void connectionPresentNoJetStreamBean_createsJetStreamViaConnectionDefaultBean() throws Exception {
        Connection connection = mock(Connection.class);
        JetStream jetStreamFromConnection = mock(JetStream.class);
        when(connection.jetStream()).thenReturn(jetStreamFromConnection);

        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(NatsHistoryProjectionAutoConfiguration.class))
                .withBean(Connection.class, () -> connection)
                .run(context -> {
                    assertThat(context).hasSingleBean(JetStream.class);
                    assertThat(context.getBean(JetStream.class)).isSameAs(jetStreamFromConnection);
                });
    }

    @Test
    void meterRegistryPresent_createsNatsChannelMetricsDefaultBean() {
        // QA-FINDING (see round-2 return report, same pattern already documented in
        // flowable-nats-channel): historyDlqInspectionConsumer() unconditionally forwards any
        // present MeterRegistry into DlqBridgeCircuitBreakerFactory.create(...), which touches
        // io.github.resilience4j.micrometer.tagged.TaggedCircuitBreakerMetrics -- an
        // <optional>true</optional> nats-core dependency this module does not redeclare either.
        // A mock HistoryDlqInspectionConsumer bean short-circuits that ConditionalOnMissingBean
        // factory method so THIS test can isolate natsChannelMetrics() itself, which never
        // touches the resilience4j-micrometer classes.
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(NatsHistoryProjectionAutoConfiguration.class))
                .withBean(Connection.class, () -> mock(Connection.class))
                .withBean(JetStream.class, () -> mock(JetStream.class))
                .withBean(HistoryDlqInspectionConsumer.class, () -> mock(HistoryDlqInspectionConsumer.class))
                .withBean(MeterRegistry.class, SimpleMeterRegistry::new)
                .run(context -> assertThat(context).hasSingleBean(NatsChannelMetrics.class));
    }

    @Test
    void projectionAndVaultDatasourceProperties_createRealHikariPoolsViaBuildPool() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(NatsHistoryProjectionAutoConfiguration.class))
                .withBean(Connection.class, () -> mock(Connection.class))
                .withBean(JetStream.class, () -> mock(JetStream.class))
                .withPropertyValues(
                        "history.projection.datasource.jdbc-url=" + postgres.getJdbcUrl(),
                        "history.projection.datasource.username=" + postgres.getUsername(),
                        "history.projection.datasource.password=" + postgres.getPassword(),
                        "history.vault.datasource.jdbc-url=" + postgres.getJdbcUrl(),
                        "history.vault.datasource.username=" + postgres.getUsername(),
                        "history.vault.datasource.password=" + postgres.getPassword()
                )
                .run(context -> {
                    assertThat(context).hasBean("projectionDataSource");
                    assertThat(context).hasBean("vaultDataSource");
                    DataSource projectionDataSource = (DataSource) context.getBean("projectionDataSource");
                    DataSource vaultDataSource = (DataSource) context.getBean("vaultDataSource");
                    try (java.sql.Connection c1 = projectionDataSource.getConnection()) {
                        assertThat(c1.isValid(2)).isTrue();
                    }
                    try (java.sql.Connection c2 = vaultDataSource.getConnection()) {
                        assertThat(c2.isValid(2)).isTrue();
                    }
                });
    }
}
