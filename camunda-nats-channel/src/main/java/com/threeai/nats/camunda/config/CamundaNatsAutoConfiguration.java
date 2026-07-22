package com.threeai.nats.camunda.config;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import javax.sql.DataSource;

import com.threeai.nats.camunda.a2.A2BpmnParseListener;
import com.threeai.nats.camunda.a2.A2PostCommitPublisher;
import com.threeai.nats.camunda.a2.A2Properties;
import com.threeai.nats.camunda.a2.A2SubscriptionRegistrar;
import com.threeai.nats.camunda.a2.A2TopicConfig;
import com.threeai.nats.camunda.a2.UmbrellaLockResolver;
import com.threeai.nats.camunda.a2.UmbrellaLockValidator;
import com.threeai.nats.camunda.history.ClassCutoverStateRegistry;
import com.threeai.nats.camunda.history.CompactHistoryOutboxWriter;
import com.threeai.nats.camunda.history.HistoryBootstrapValidator;
import com.threeai.nats.camunda.history.HistoryClassificationProperties;
import com.threeai.nats.camunda.history.HistoryOutboxProperties;
import com.threeai.nats.camunda.history.HistoryOutboxRelay;
import com.threeai.nats.camunda.history.HistoryOutboxRelayScheduler;
import com.threeai.nats.camunda.history.HistoryPostCommitPublisher;
import com.threeai.nats.camunda.history.NatsHistoryEventHandler;
import com.threeai.nats.camunda.variable.LargeVariableExternalizationSweep;
import com.threeai.nats.camunda.variable.LargeVariablePostCommitExternalizer;
import com.threeai.nats.camunda.variable.LargeVariableSerializer;
import com.threeai.nats.core.NatsConnectionFactory;
import com.threeai.nats.core.NatsProperties;
import com.threeai.nats.core.config.NatsTransportSecurityGuard;
import com.threeai.nats.core.dlq.DlqPublisher;
import com.threeai.nats.core.history.PseudonymTokenGenerator;
import com.threeai.nats.core.jetstream.JetStreamKvManager;
import com.threeai.nats.core.jetstream.JetStreamStreamManager;
import com.threeai.nats.core.jetstream.SweepLeaderLease;
import com.threeai.nats.core.largepayload.ContentAddressedLargePayloadStore;
import com.threeai.nats.core.largepayload.LargeVariableExternalizationProperties;
import com.threeai.nats.core.largepayload.LargeVariableProjectionDataSourceProperties;
import com.threeai.nats.core.largepayload.LargeVariableSerializerNames;
import com.threeai.nats.core.metrics.NatsChannelMetrics;
import io.micrometer.core.instrument.MeterRegistry;
import io.nats.client.Connection;
import io.nats.client.JetStream;
import org.camunda.bpm.engine.ExternalTaskService;
import org.camunda.bpm.engine.ProcessEngine;
import org.camunda.bpm.engine.RuntimeService;
import org.camunda.bpm.engine.impl.cfg.AbstractProcessEnginePlugin;
import org.camunda.bpm.engine.impl.cfg.ProcessEngineConfigurationImpl;
import org.camunda.bpm.engine.impl.cfg.ProcessEnginePlugin;
import org.camunda.bpm.engine.impl.history.handler.DbHistoryEventHandler;
import org.camunda.bpm.engine.impl.history.handler.HistoryEventHandler;
import org.camunda.bpm.engine.impl.variable.serializer.ByteArrayValueSerializer;
import org.camunda.bpm.engine.impl.variable.serializer.FileValueSerializer;
import org.camunda.bpm.engine.impl.variable.serializer.JavaObjectSerializer;
import org.camunda.bpm.engine.impl.variable.serializer.TypedValueSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;

@AutoConfiguration
@ConditionalOnClass(org.camunda.bpm.engine.ProcessEngine.class)
@EnableConfigurationProperties({NatsProperties.class, CamundaNatsProperties.class, A2Properties.class,
        HistoryClassificationProperties.class, HistoryOutboxProperties.class,
        com.threeai.nats.core.vault.PseudonymVaultDataSourceProperties.class,
        LargeVariableExternalizationProperties.class, LargeVariableProjectionDataSourceProperties.class})
public class CamundaNatsAutoConfiguration {

    private static final String ENGINE_ID = "camunda";
    private static final String RELAY_LEADER_BUCKET = "history-relay-leader";
    private static final String LARGE_VARIABLE_LEADER_BUCKET = "large-variable-sweep-leader";
    private static final String RELAY_LEADER_KEY_PREFIX = "relay-leader.";
    private static final String CUTOVER_STATE_BUCKET = "history-cutover-state";

    @Bean(destroyMethod = "close")
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
    public JetStreamStreamManager jetStreamStreamManager() {
        return new JetStreamStreamManager();
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
    public NatsTransportSecurityGuard natsTransportSecurityGuard(NatsProperties props, Environment environment) {
        return new NatsTransportSecurityGuard(props, environment);
    }

    @Bean
    @ConditionalOnMissingBean
    public DlqPublisher dlqPublisher(JetStream jetStream, Connection connection,
            @Autowired(required = false) NatsChannelMetrics metrics) {
        return new DlqPublisher(jetStream, connection, metrics);
    }

    @Bean
    public NatsSubscriptionRegistrar natsSubscriptionRegistrar(
            CamundaNatsProperties properties,
            Connection connection,
            JetStream jetStream,
            JetStreamStreamManager streamManager,
            RuntimeService runtimeService,
            @Autowired(required = false) NatsChannelMetrics metrics,
            DlqPublisher dlqPublisher) {
        return new NatsSubscriptionRegistrar(
                properties, connection, jetStream, streamManager, runtimeService, metrics, dlqPublisher);
    }

    // --- A2 (basamak-1) ---

    @Bean
    @ConditionalOnMissingBean
    public A2TopicConfig a2TopicConfig(A2Properties a2Properties) {
        return new A2TopicConfig(a2Properties);
    }

    @Bean
    @ConditionalOnMissingBean
    public UmbrellaLockResolver umbrellaLockResolver(A2Properties a2Properties) {
        return new UmbrellaLockResolver(a2Properties);
    }

    @Bean
    @ConditionalOnMissingBean
    public UmbrellaLockValidator umbrellaLockValidator(A2Properties a2Properties, UmbrellaLockResolver resolver) {
        return new UmbrellaLockValidator(a2Properties, resolver);
    }

    @Bean
    @ConditionalOnMissingBean
    public A2PostCommitPublisher a2PostCommitPublisher(JetStream jetStream,
            @Autowired(required = false) NatsChannelMetrics metrics, UmbrellaLockValidator lockValidator) {
        return new A2PostCommitPublisher(jetStream, metrics, lockValidator);
    }

    @Bean
    @ConditionalOnMissingBean
    public A2BpmnParseListener a2BpmnParseListener(A2TopicConfig topicConfig, A2Properties a2Properties,
            UmbrellaLockResolver lockResolver, A2PostCommitPublisher publisher) {
        return new A2BpmnParseListener(topicConfig, a2Properties.getSentinelWorkerId(), lockResolver, publisher);
    }

    @Bean
    public ProcessEnginePlugin a2ProcessEnginePlugin(A2BpmnParseListener listener) {
        return new AbstractProcessEnginePlugin() {
            @Override
            public void preInit(ProcessEngineConfigurationImpl configuration) {
                configuration.getPreParseListeners().add(listener);
            }
        };
    }

    @Bean
    @ConditionalOnMissingBean
    public A2SubscriptionRegistrar a2SubscriptionRegistrar(A2Properties a2Properties, Connection connection,
            JetStream jetStream, ExternalTaskService externalTaskService, DlqPublisher dlqPublisher,
            @Autowired(required = false) NatsChannelMetrics metrics,
            @Autowired(required = false) MeterRegistry meterRegistry,
            ProcessEngine processEngine, UmbrellaLockResolver lockResolver, A2TopicConfig topicConfig,
            UmbrellaLockValidator lockValidator, JetStreamKvManager kvManager) {
        return new A2SubscriptionRegistrar(a2Properties, connection, jetStream, externalTaskService, dlqPublisher,
                metrics, meterRegistry, processEngine, lockResolver, topicConfig, lockValidator, kvManager);
    }

    // --- History Offload (basamak-2) ---

    @Bean
    @ConditionalOnMissingBean
    public PseudonymTokenGenerator pseudonymTokenGenerator() {
        return new PseudonymTokenGenerator();
    }

    /**
     * CQ-1: engine-side pseudonym-vault write path (ADR-0016 "persist downstream/async").
     * Physically SEPARATE Postgres pool (ARCH-Q2) — never shares {@code dataSource} (the engine
     * DB). Gated on {@code history.vault.datasource.jdbc-url} being configured at all:
     * pseudonymization opt-in without a configured vault is a valid (if unusual) tenant choice
     * (see {@code CompactHistoryOutboxWriter}'s CODER-NOTE) — this bean simply does not exist in
     * that case, and {@code compactHistoryOutboxWriter} below falls back to its
     * vault-less constructor.
     */
    @Bean(name = "pseudonymVaultDataSource", destroyMethod = "close")
    @ConditionalOnMissingBean(name = "pseudonymVaultDataSource")
    @org.springframework.boot.autoconfigure.condition.ConditionalOnProperty(
            prefix = "history.vault.datasource", name = "jdbc-url")
    public DataSource pseudonymVaultDataSource(com.threeai.nats.core.vault.PseudonymVaultDataSourceProperties props) {
        com.zaxxer.hikari.HikariConfig config = new com.zaxxer.hikari.HikariConfig();
        config.setJdbcUrl(props.getJdbcUrl());
        config.setUsername(props.getUsername());
        config.setPassword(props.getPassword());
        config.setPoolName("camunda-pseudonym-vault-pool");
        return new com.zaxxer.hikari.HikariDataSource(config);
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnBean(name = "pseudonymVaultDataSource")
    public com.threeai.nats.core.vault.VaultAccessAuditor vaultAccessAuditor(
            @org.springframework.beans.factory.annotation.Qualifier("pseudonymVaultDataSource") DataSource vaultDataSource) {
        return new com.threeai.nats.core.vault.VaultAccessAuditor(vaultDataSource);
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnBean(name = "pseudonymVaultDataSource")
    public com.threeai.nats.core.vault.PseudonymizationVaultClient pseudonymizationVaultClient(
            @org.springframework.beans.factory.annotation.Qualifier("pseudonymVaultDataSource") DataSource vaultDataSource,
            com.threeai.nats.core.vault.VaultAccessAuditor auditor,
            com.threeai.nats.core.vault.PseudonymVaultDataSourceProperties props) {
        return new com.threeai.nats.core.vault.PseudonymizationVaultClient(
                vaultDataSource, auditor, props.getVaultColumnEncryptionKeyRef());
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnBean(DataSource.class)
    public CompactHistoryOutboxWriter compactHistoryOutboxWriter(DataSource dataSource,
            PseudonymTokenGenerator pseudonymGenerator, HistoryClassificationProperties classification,
            @Autowired(required = false) NatsChannelMetrics metrics,
            @Autowired(required = false) com.threeai.nats.core.vault.PseudonymizationVaultClient vaultClient) {
        return new CompactHistoryOutboxWriter(dataSource, pseudonymGenerator, classification, metrics, vaultClient);
    }

    @Bean
    @ConditionalOnMissingBean
    public HistoryPostCommitPublisher historyPostCommitPublisher(JetStream jetStream,
            @Autowired(required = false) NatsChannelMetrics metrics) {
        return new HistoryPostCommitPublisher(jetStream, metrics);
    }

    /**
     * Ensures {@code history-cutover-state} and loads it ONCE at bean-creation time (LLD-Q3,
     * boot-read). {@code @ConditionalOnBean(DataSource.class)}: without an engine DataSource
     * there is no {@code compact_history_outbox} either, so cutover-state has nothing to route
     * (mirrors {@link #compactHistoryOutboxWriter} / {@link #historyOutboxRelay} gating).
     */
    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnBean(DataSource.class)
    public ClassCutoverStateRegistry classCutoverStateRegistry(JetStreamKvManager kvManager, Connection connection) {
        kvManager.ensureBucket(CUTOVER_STATE_BUCKET, Duration.ZERO, 3, connection);
        ClassCutoverStateRegistry registry = new ClassCutoverStateRegistry(kvManager, connection, ENGINE_ID);
        registry.loadAtBootstrap();
        return registry;
    }

    /** Ensures {@code history-relay-leader} — separate bucket/key namespace from {@code a2-sweep-leader}. */
    @Bean
    @ConditionalOnMissingBean(name = "historyRelayLeaderLease")
    @ConditionalOnBean(DataSource.class)
    public SweepLeaderLease historyRelayLeaderLease(JetStream jetStream, JetStreamKvManager kvManager,
            Connection connection, HistoryOutboxProperties outboxProperties) {
        Duration ttl = Duration.ofSeconds(2 * outboxProperties.getRelayCyclePeriodSeconds());
        kvManager.ensureBucket(RELAY_LEADER_BUCKET, ttl, 3, connection);
        return new SweepLeaderLease(jetStream, kvManager, connection, RELAY_LEADER_BUCKET,
                RELAY_LEADER_KEY_PREFIX, ENGINE_ID, resolveNodeId(), ttl);
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnBean(DataSource.class)
    public HistoryOutboxRelay historyOutboxRelay(DataSource dataSource, JetStream jetStream,
            SweepLeaderLease historyRelayLeaderLease, HistoryOutboxProperties outboxProperties,
            @Autowired(required = false) NatsChannelMetrics metrics) {
        return new HistoryOutboxRelay(dataSource, jetStream, historyRelayLeaderLease, outboxProperties, metrics, ENGINE_ID);
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnBean(DataSource.class)
    public HistoryOutboxRelayScheduler historyOutboxRelayScheduler(HistoryOutboxRelay historyOutboxRelay,
            HistoryOutboxProperties outboxProperties) {
        return new HistoryOutboxRelayScheduler(historyOutboxRelay, outboxProperties.getRelayCyclePeriodSeconds(), ENGINE_ID);
    }

    /** Requires {@link ClassCutoverStateRegistry}, which is itself gated on a DataSource bean. */
    @Bean
    @ConditionalOnBean(DataSource.class)
    public ProcessEnginePlugin historyProcessEnginePlugin(ClassCutoverStateRegistry cutoverStateRegistry,
            HistoryClassificationProperties classification,
            @Autowired(required = false) CompactHistoryOutboxWriter outboxWriter,
            HistoryPostCommitPublisher postCommitPublisher) {
        return new AbstractProcessEnginePlugin() {
            @Override
            public void preInit(ProcessEngineConfigurationImpl configuration) {
                HistoryBootstrapValidator.validate(configuration, classification, ENGINE_ID);
                NatsHistoryEventHandler handler = new NatsHistoryEventHandler(cutoverStateRegistry, classification,
                        outboxWriter, postCommitPublisher, new DbHistoryEventHandler(), ENGINE_ID);
                // enableDefaultDbHistoryEventHandler ALWAYS false -- our composite owns its own
                // internalDbDelegate (01_overview.md "Phase3'ün devrettiği doğrulamalar #1").
                configuration.setEnableDefaultDbHistoryEventHandler(false);
                List<HistoryEventHandler> customHandlers = new ArrayList<>(configuration.getCustomHistoryEventHandlers());
                customHandlers.add(handler);
                configuration.setCustomHistoryEventHandlers(customHandlers);
            }
        };
    }

    // --- Large Variable Externalization (basamak-3) ---

    /**
     * Separate Hikari pool pointed at the SAME physical projection Postgres instance {@code
     * nats-history-projection} owns (D-B'/D-D' unified store — see {@code
     * LargeVariableProjectionDataSourceProperties} class Javadoc for the ARCH-Q2-style isolation
     * rationale). Gated on {@code history.large-variable.projection-datasource.jdbc-url} being
     * configured at all — without it, externalization simply never activates for this deployment
     * (D-C' {@code enabled} kill-switch is a SEPARATE, always-available runtime toggle).
     */
    @Bean(name = "largeVariableProjectionDataSource", destroyMethod = "close")
    @ConditionalOnMissingBean(name = "largeVariableProjectionDataSource")
    @ConditionalOnProperty(prefix = "history.large-variable.projection-datasource", name = "jdbc-url")
    public DataSource largeVariableProjectionDataSource(LargeVariableProjectionDataSourceProperties props) {
        com.zaxxer.hikari.HikariConfig config = new com.zaxxer.hikari.HikariConfig();
        config.setJdbcUrl(props.getJdbcUrl());
        config.setUsername(props.getUsername());
        config.setPassword(props.getPassword());
        config.setPoolName("camunda-large-variable-projection-pool");
        return new com.zaxxer.hikari.HikariDataSource(config);
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnBean(name = "largeVariableProjectionDataSource")
    public ContentAddressedLargePayloadStore largeVariablePayloadStore(
            @org.springframework.beans.factory.annotation.Qualifier("largeVariableProjectionDataSource") DataSource largeVariableProjectionDataSource) {
        return new ContentAddressedLargePayloadStore(largeVariableProjectionDataSource);
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnBean(ContentAddressedLargePayloadStore.class)
    public LargeVariablePostCommitExternalizer largeVariablePostCommitExternalizer(
            ContentAddressedLargePayloadStore largeVariablePayloadStore, LargeVariableExternalizationProperties properties,
            @Autowired(required = false) NatsChannelMetrics metrics) {
        return new LargeVariablePostCommitExternalizer(largeVariablePayloadStore, properties, metrics, ENGINE_ID);
    }

    /**
     * Registers the 3 BYTES/OBJECT/FILE decorators into {@code customPreVariableSerializers}
     * (docs/08 §2.1 evidence: the fork scans this list FIRST and stops at the first match, so no
     * fork change is needed for these to win over the built-ins they wrap). {@code
     * postCommitExternalizer.bindConfiguration(configuration)} here is what makes {@link
     * LargeVariablePostCommitExternalizer} usable later without a circular {@code ProcessEngine}
     * bean dependency — see that class's own CODER-NOTE.
     */
    @Bean
    @ConditionalOnBean(LargeVariablePostCommitExternalizer.class)
    public ProcessEnginePlugin largeVariableProcessEnginePlugin(
            LargeVariablePostCommitExternalizer postCommitExternalizer,
            LargeVariableExternalizationProperties properties, ContentAddressedLargePayloadStore largeVariablePayloadStore) {
        return new AbstractProcessEnginePlugin() {
            @Override
            public void preInit(ProcessEngineConfigurationImpl configuration) {
                postCommitExternalizer.bindConfiguration(configuration);

                List<TypedValueSerializer> customSerializers = configuration.getCustomPreVariableSerializers() != null
                        ? new ArrayList<>(configuration.getCustomPreVariableSerializers())
                        : new ArrayList<>();
                customSerializers.add(new LargeVariableSerializer<>(new ByteArrayValueSerializer(),
                        LargeVariableSerializerNames.BYTES, properties, largeVariablePayloadStore, postCommitExternalizer));
                customSerializers.add(new LargeVariableSerializer<>(new JavaObjectSerializer(),
                        LargeVariableSerializerNames.OBJECT, properties, largeVariablePayloadStore, postCommitExternalizer));
                customSerializers.add(new LargeVariableSerializer<>(new FileValueSerializer(),
                        LargeVariableSerializerNames.FILE, properties, largeVariablePayloadStore, postCommitExternalizer));
                configuration.setCustomPreVariableSerializers(customSerializers);
            }
        };
    }

    /** Ensures {@code large-variable-sweep-leader} — separate bucket/key namespace from the other two leases. */
    @Bean
    @ConditionalOnMissingBean(name = "largeVariableSweepLeaderLease")
    @ConditionalOnBean(LargeVariablePostCommitExternalizer.class)
    public SweepLeaderLease largeVariableSweepLeaderLease(JetStream jetStream, JetStreamKvManager kvManager,
            Connection connection, LargeVariableExternalizationProperties properties) {
        Duration ttl = Duration.ofSeconds(2 * properties.getSweepCyclePeriodSeconds());
        kvManager.ensureBucket(LARGE_VARIABLE_LEADER_BUCKET, ttl, 3, connection);
        return new SweepLeaderLease(jetStream, kvManager, connection, LARGE_VARIABLE_LEADER_BUCKET,
                "sweep-leader.", ENGINE_ID, resolveNodeId(), ttl);
    }

    /** {@code largeVariableSweepLeaderLease}/{@code largeVariablePostCommitExternalizer} bean-NAME
     *  gating (not type — {@code SweepLeaderLease} also has an unrelated {@code
     *  historyRelayLeaderLease} instance in this SAME configuration): only activates when a
     *  projection {@code DataSource} is actually configured (see those beans' own conditions). */
    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnBean(value = DataSource.class, name = {"largeVariableSweepLeaderLease", "largeVariablePostCommitExternalizer"})
    public LargeVariableExternalizationSweep largeVariableExternalizationSweep(DataSource dataSource,
            SweepLeaderLease largeVariableSweepLeaderLease,
            LargeVariablePostCommitExternalizer largeVariablePostCommitExternalizer,
            ContentAddressedLargePayloadStore largeVariablePayloadStore,
            LargeVariableExternalizationProperties properties) {
        return new LargeVariableExternalizationSweep(dataSource, largeVariableSweepLeaderLease,
                largeVariablePostCommitExternalizer, largeVariablePayloadStore, properties, ENGINE_ID);
    }

    private static String resolveNodeId() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            return "node-" + UUID.randomUUID();
        }
    }
}
