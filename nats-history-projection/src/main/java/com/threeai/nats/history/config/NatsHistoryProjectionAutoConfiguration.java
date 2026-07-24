package com.threeai.nats.history.config;

import java.io.IOException;
import javax.sql.DataSource;

import com.threeai.nats.core.NatsConnectionFactory;
import com.threeai.nats.core.NatsProperties;
import com.threeai.nats.core.dlq.DlqPublisher;
import com.threeai.nats.core.history.PseudonymTokenGenerator;
import com.threeai.nats.core.jetstream.JetStreamKvManager;
import com.threeai.nats.core.metrics.NatsChannelMetrics;
import com.threeai.nats.core.resilience.DlqBridgeCircuitBreakerFactory;
import com.threeai.nats.core.vault.PseudonymVaultDataSourceProperties;
import com.threeai.nats.core.vault.PseudonymizationVaultClient;
import com.threeai.nats.core.vault.VaultAccessAuditor;
import com.threeai.nats.history.cutover.ClassCutoverStateStore;
import com.threeai.nats.history.cutover.CutoverControlPlane;
import com.threeai.nats.history.cutover.CutoverRollback;
import com.threeai.nats.history.cutover.HistoryCutoverProperties;
import com.threeai.nats.history.cutover.ReconciliationJob;
import com.threeai.nats.history.cutover.ReconciliationProperties;
import com.threeai.nats.history.governance.ErasureAuditLogger;
import com.threeai.nats.history.governance.ErasurePipeline;
import com.threeai.nats.history.governance.ErasureScopeResolver;
import com.threeai.nats.history.governance.RetentionAuditLogger;
import com.threeai.nats.history.governance.RetentionEnforcementJob;
import com.threeai.nats.history.governance.RetentionProperties;
import com.threeai.nats.history.projection.HistoryDlqConsumer;
import com.threeai.nats.history.projection.HistoryDlqInspectionConsumer;
import com.threeai.nats.history.projection.HistoryDlqInspectionSubscriptionRegistrar;
import com.threeai.nats.history.projection.HistoryProjectionConsumerBootstrap;
import com.threeai.nats.history.projection.HistoryProjectionDataSourceProperties;
import com.threeai.nats.history.projection.HistoryProjectionProperties;
import com.threeai.nats.history.projection.ProjectionStore;
import com.threeai.nats.history.query.HistoryQueryApi;
import com.threeai.nats.history.query.HistoryQueryAuthzSpi;
import com.threeai.nats.history.query.HistoryQueryController;
import com.threeai.nats.history.query.PiiMaskingService;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.micrometer.core.instrument.MeterRegistry;
import io.nats.client.Connection;
import io.nats.client.JetStream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Wires the engine-neutral {@code nats-history-projection} module (`02_package_structure.md`).
 * Mirrors {@code CamundaNatsAutoConfiguration}'s conditional-wiring discipline: every bean that
 * depends on a physical Postgres instance is gated on that {@code DataSource} bean's presence, so
 * the module degrades gracefully (no-op) rather than failing bootstrap when a tenant only wants a
 * subset of the feature (e.g. query-only, no retention/erasure).
 *
 * <p><b>CODER-NOTE (own DataSource pooling, no Flyway/Liquibase wiring here):</b> per {@code
 * SqlMigrationRunner}'s own class-level CODER-NOTE, applying {@code db/migration/projection/*} and
 * {@code db/migration/vault/*} to the target Postgres instances is a TENANT deploy-time concern
 * (their own Flyway/Liquibase changelog), consistent with how {@code CamundaNatsAutoConfiguration}
 * already treats {@code db/migration/history/V1__compact_history_outbox.sql} — this
 * auto-configuration only builds connection pools, never applies schema DDL itself.
 *
 * <p><b>CODER-NOTE (fail-closed query/erasure surface):</b> {@link HistoryQueryAuthzSpi} has no
 * default "allow all" implementation registered here — {@link HistoryQueryApi} (and therefore
 * {@link ErasurePipeline}, which needs it for post-anonymization verification) only activates once
 * the tenant supplies their own authz SPI bean (ARCH-Q4, SRS §4.7), never silently open.
 *
 * <p><b>CQ-2/CQ-5 (Levent, önerilen 2026-07-20 — multi-engine scope, resolved):</b> {@link
 * ReconciliationJob} and {@link RetentionEnforcementJob} each bind to exactly ONE {@code
 * engineId} ({@code history.engine-id}, default {@code "camunda"} — a property {@code
 * 08_config.md} does not itself define). This stays AS-IS by design — the default auto-config
 * targets the primary single-engine deployment shape. A deployment running BOTH {@code
 * camunda-nats-channel} and {@code cadenzaflow-nats-channel} simultaneously against the SAME
 * projection store declares a SECOND pair of beans with an overridden {@code engineId} in its own
 * {@code @Configuration} — this module does not attempt that fan-out automatically, by decision,
 * not oversight. Full recipe: {@code nats-history-projection/README.md} "çoklu-motor dağıtım
 * reçetesi" (CQ-2/CQ-5). The related engineId-not-in-query-API constraint is a SEPARATE, also
 * explicitly accepted limitation — see {@code HistoryQueryApi}'s own CODER-NOTE.
 */
@AutoConfiguration
@EnableScheduling
@EnableConfigurationProperties({NatsProperties.class, HistoryProjectionProperties.class,
        HistoryProjectionDataSourceProperties.class, PseudonymVaultDataSourceProperties.class,
        ReconciliationProperties.class, HistoryCutoverProperties.class, RetentionProperties.class})
public class NatsHistoryProjectionAutoConfiguration {

    /**
     * QA-FINDING-2 fix: {@code NatsProperties} is now bound by THIS auto-configuration's own
     * {@code @EnableConfigurationProperties} above (mirrors {@code FlowableNatsAutoConfiguration}'s
     * discipline) instead of relying on a co-located engine module's auto-configuration to have
     * bound it first. A tenant running this module standalone (no camunda/flowable-nats-channel
     * on the classpath) without supplying their own {@code Connection} bean previously hit {@code
     * NoSuchBeanDefinitionException} here.
     */
    @Bean
    @ConditionalOnMissingBean
    public Connection natsConnection(NatsProperties props) throws IOException, InterruptedException {
        return NatsConnectionFactory.create(props);
    }

    @Bean
    @ConditionalOnMissingBean
    public JetStream natsJetStream(Connection connection) throws IOException {
        return connection.jetStream();
    }

    @Bean
    @ConditionalOnMissingBean
    public JetStreamKvManager jetStreamKvManager() {
        return new JetStreamKvManager();
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnBean(MeterRegistry.class)
    public NatsChannelMetrics natsChannelMetrics(MeterRegistry registry) {
        return new NatsChannelMetrics(registry);
    }

    @Bean
    @ConditionalOnMissingBean
    public DlqPublisher dlqPublisher(JetStream jetStream, Connection connection,
            @Autowired(required = false) NatsChannelMetrics metrics) {
        return new DlqPublisher(jetStream, connection, metrics);
    }

    @Bean
    @ConditionalOnMissingBean
    public PseudonymTokenGenerator pseudonymTokenGenerator() {
        return new PseudonymTokenGenerator();
    }

    @Bean
    @ConditionalOnMissingBean
    public PiiMaskingService piiMaskingService() {
        return new PiiMaskingService();
    }

    // --- DataSources (ARCH-Q2: two physically separate pools) ---

    @Bean(name = "projectionDataSource", destroyMethod = "close")
    @ConditionalOnMissingBean(name = "projectionDataSource")
    @ConditionalOnProperty(prefix = "history.projection.datasource", name = "jdbc-url")
    public DataSource projectionDataSource(HistoryProjectionDataSourceProperties props) {
        return buildPool(props.getJdbcUrl(), props.getUsername(), props.getPassword(), "history-projection-pool");
    }

    @Bean(name = "vaultDataSource", destroyMethod = "close")
    @ConditionalOnMissingBean(name = "vaultDataSource")
    @ConditionalOnProperty(prefix = "history.vault.datasource", name = "jdbc-url")
    public DataSource vaultDataSource(PseudonymVaultDataSourceProperties props) {
        return buildPool(props.getJdbcUrl(), props.getUsername(), props.getPassword(), "history-vault-pool");
    }

    private DataSource buildPool(String jdbcUrl, String username, String password, String poolName) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(jdbcUrl);
        config.setUsername(username);
        config.setPassword(password);
        config.setPoolName(poolName);
        return new HikariDataSource(config);
    }

    // --- Projection consumer path ---

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnBean(name = "projectionDataSource")
    public ProjectionStore projectionStore(@Qualifier("projectionDataSource") DataSource projectionDataSource) {
        return new ProjectionStore(projectionDataSource);
    }

    @Bean
    @ConditionalOnMissingBean
    public HistoryDlqConsumer historyDlqConsumer(DlqPublisher dlqPublisher,
            @Autowired(required = false) NatsChannelMetrics metrics) {
        return new HistoryDlqConsumer(dlqPublisher, metrics);
    }

    @Bean
    @ConditionalOnMissingBean
    public HistoryDlqInspectionConsumer historyDlqInspectionConsumer(
            @Autowired(required = false) MeterRegistry meterRegistry) {
        CircuitBreaker cb = DlqBridgeCircuitBreakerFactory.create("cb-history-dlq-inspection", meterRegistry);
        return new HistoryDlqInspectionConsumer(cb);
    }

    @Bean
    @ConditionalOnMissingBean
    public HistoryDlqInspectionSubscriptionRegistrar historyDlqInspectionSubscriptionRegistrar(
            Connection connection, JetStream jetStream, HistoryDlqInspectionConsumer consumer) {
        return new HistoryDlqInspectionSubscriptionRegistrar(connection, jetStream, consumer);
    }

    @Bean
    @ConditionalOnBean(name = "vaultDataSource")
    public VaultAccessAuditor vaultAccessAuditor(@Qualifier("vaultDataSource") DataSource vaultDataSource) {
        return new VaultAccessAuditor(vaultDataSource);
    }

    /**
     * CODER-NOTE: {@code vaultColumnEncryptionKeyRef} is documented as a REFERENCE (OpenBao/deploy
     * secret path), not the literal key material (`08_config.md §7`). No OpenBao SDK dependency
     * exists in this repo (`PseudonymTokenGenerator`'s own CODER-NOTE states the same secret-
     * resolution boundary) — this bean passes the configured value straight through, i.e. it
     * assumes the tenant's deployment tooling resolves the reference INTO this property (env-var
     * injection / k8s secret mount) before the process starts, rather than this module performing
     * a runtime lookup itself.
     */
    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnBean(name = "vaultDataSource")
    public PseudonymizationVaultClient pseudonymizationVaultClient(@Qualifier("vaultDataSource") DataSource vaultDataSource,
            VaultAccessAuditor auditor, PseudonymVaultDataSourceProperties props) {
        return new PseudonymizationVaultClient(vaultDataSource, auditor, props.getVaultColumnEncryptionKeyRef());
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnBean(name = "projectionDataSource")
    public HistoryProjectionConsumerBootstrap historyProjectionConsumerBootstrap(JetStream jetStream,
            Connection connection, ProjectionStore projectionStore, HistoryDlqConsumer dlqConsumer,
            @Autowired(required = false) NatsChannelMetrics metrics, HistoryProjectionProperties properties) {
        return new HistoryProjectionConsumerBootstrap(jetStream, connection, projectionStore, dlqConsumer,
                metrics, properties);
    }

    // --- Query API (ARCH-Q4: embeddable + optional standalone REST) ---

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnBean(value = HistoryQueryAuthzSpi.class, name = "projectionDataSource")
    public HistoryQueryApi historyQueryApi(@Qualifier("projectionDataSource") DataSource projectionDataSource,
            PiiMaskingService maskingService, HistoryQueryAuthzSpi authzSpi) {
        return new HistoryQueryApi(projectionDataSource, maskingService, authzSpi);
    }

    /** Optional standalone REST exposure — embeddable-library mode leaves this off (ARCH-Q4). */
    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnBean(HistoryQueryApi.class)
    @ConditionalOnProperty(prefix = "history.query", name = "standalone-rest-enabled", havingValue = "true")
    public HistoryQueryController historyQueryController(HistoryQueryApi historyQueryApi, HistoryQueryAuthzSpi authzSpi) {
        return new HistoryQueryController(historyQueryApi, authzSpi);
    }

    // --- Cutover / reconciliation ---

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnBean(name = "projectionDataSource")
    public ClassCutoverStateStore classCutoverStateStore(@Qualifier("projectionDataSource") DataSource projectionDataSource) {
        return new ClassCutoverStateStore(projectionDataSource);
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnBean(name = "projectionDataSource")
    public CutoverControlPlane cutoverControlPlane(ClassCutoverStateStore stateStore, JetStreamKvManager kvManager,
            HistoryCutoverProperties properties, Connection connection) {
        return new CutoverControlPlane(stateStore, kvManager, properties, connection);
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnBean(name = "projectionDataSource")
    public CutoverRollback cutoverRollback(ClassCutoverStateStore stateStore, JetStreamKvManager kvManager,
            Connection connection) {
        return new CutoverRollback(stateStore, kvManager, connection);
    }

    /**
     * CODER-NOTE: requires a {@code DataSource} bean literally named {@code
     * engineDataSourceReadOnly} — a read-only connection to the ENGINE's own Postgres (ACT_HI
     * tables), which this engine-neutral module does not itself own or create (`08_config.md`
     * gives no property prefix for it). Not created unless the embedding application supplies
     * that bean (see class-level CODER-QUESTION re: multi-engine fan-out).
     */
    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnBean(name = {"projectionDataSource", "engineDataSourceReadOnly"})
    public ReconciliationJob reconciliationJob(@Qualifier("projectionDataSource") DataSource projectionDataSource,
            @Qualifier("engineDataSourceReadOnly") DataSource engineDataSourceReadOnly,
            ClassCutoverStateStore stateStore, @Autowired(required = false) NatsChannelMetrics metrics,
            ReconciliationProperties properties, @Value("${history.engine-id:camunda}") String engineId) {
        return new ReconciliationJob(projectionDataSource, engineDataSourceReadOnly, stateStore, metrics,
                properties, engineId);
    }

    // --- Retention / erasure governance ---

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnBean(name = "projectionDataSource")
    public RetentionAuditLogger retentionAuditLogger(@Qualifier("projectionDataSource") DataSource projectionDataSource) {
        return new RetentionAuditLogger(projectionDataSource);
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnBean(name = "projectionDataSource")
    public RetentionEnforcementJob retentionEnforcementJob(@Qualifier("projectionDataSource") DataSource projectionDataSource,
            RetentionAuditLogger auditLogger, RetentionProperties properties,
            @Value("${history.engine-id:camunda}") String engineId) {
        return new RetentionEnforcementJob(projectionDataSource, auditLogger, properties, engineId);
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnBean(name = "projectionDataSource")
    public ErasureScopeResolver erasureScopeResolver(@Qualifier("projectionDataSource") DataSource projectionDataSource) {
        return new ErasureScopeResolver(projectionDataSource);
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnBean(name = "projectionDataSource")
    public ErasureAuditLogger erasureAuditLogger(@Qualifier("projectionDataSource") DataSource projectionDataSource) {
        return new ErasureAuditLogger(projectionDataSource);
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnBean({HistoryQueryApi.class})
    public ErasurePipeline erasurePipeline(@Qualifier("projectionDataSource") DataSource projectionDataSource,
            ErasureScopeResolver scopeResolver, ErasureAuditLogger auditLogger, HistoryQueryApi verificationQuery) {
        return new ErasurePipeline(projectionDataSource, scopeResolver, auditLogger, verificationQuery);
    }
}
