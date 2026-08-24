package com.threeai.nats.cibseven.config;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import javax.sql.DataSource;

import com.threeai.nats.cibseven.a2.A2BpmnParseListener;
import com.threeai.nats.cibseven.a2.A2PostCommitPublisher;
import com.threeai.nats.cibseven.a2.A2Properties;
import com.threeai.nats.cibseven.a2.A2SubscriptionRegistrar;
import com.threeai.nats.cibseven.a2.A2TopicConfig;
import com.threeai.nats.cibseven.a2.UmbrellaLockResolver;
import com.threeai.nats.cibseven.a2.UmbrellaLockValidator;
import com.threeai.nats.cibseven.history.ClassCutoverStateRegistry;
import com.threeai.nats.cibseven.history.CompactHistoryOutboxWriter;
import com.threeai.nats.cibseven.history.HistoryBootstrapValidator;
import com.threeai.nats.cibseven.history.HistoryClassificationProperties;
import com.threeai.nats.cibseven.history.HistoryOutboxProperties;
import com.threeai.nats.cibseven.history.HistoryOutboxRelay;
import com.threeai.nats.cibseven.history.HistoryOutboxRelayScheduler;
import com.threeai.nats.cibseven.history.HistoryPostCommitPublisher;
import com.threeai.nats.cibseven.history.NatsHistoryEventHandler;
import com.threeai.nats.cibseven.outbound.NatsOutboundPublisher;
import com.threeai.nats.cibseven.variable.LargeVariableExternalizationSweep;
import com.threeai.nats.cibseven.variable.LargeVariablePostCommitExternalizer;
import com.threeai.nats.cibseven.variable.LargeVariableSerializer;
import com.threeai.nats.core.NatsConnectionFactory;
import com.threeai.nats.core.NatsProperties;
import com.threeai.nats.core.config.NatsTransportSecurityGuard;
import com.threeai.nats.core.dlq.DlqPublisher;
import com.threeai.nats.core.history.PseudonymTokenGenerator;
import com.threeai.nats.core.jetstream.JetStreamKvManager;
import com.threeai.nats.core.jetstream.JetStreamTopologyCheck;
import com.threeai.nats.core.jetstream.NatsTopologySelfCheck;
import com.threeai.nats.core.jetstream.JetStreamStreamManager;
import com.threeai.nats.core.jetstream.SweepLeaderLease;
import com.threeai.nats.core.largepayload.ContentAddressedLargePayloadStore;
import com.threeai.nats.core.largepayload.LargeVariableExternalizationProperties;
import com.threeai.nats.core.largepayload.LargeVariableProjectionDataSourceProperties;
import com.threeai.nats.core.largepayload.LargeVariableSerializerNames;
import com.threeai.nats.core.metrics.NatsChannelMetrics;
import com.threeai.nats.core.outbound.OutboundClassificationProperties;
import com.threeai.nats.core.outbound.OutboundMessageOutboxProperties;
import com.threeai.nats.core.outbound.OutboundMessageOutboxWriter;
import com.threeai.nats.core.outbound.OutboundMessageRelay;
import com.threeai.nats.core.outbound.OutboundMessageRelayScheduler;
import com.threeai.nats.core.outbound.OutboundPostCommitPublisher;
import io.micrometer.core.instrument.MeterRegistry;
import io.nats.client.Connection;
import io.nats.client.JetStream;
import org.cibseven.bpm.engine.ExternalTaskService;
import org.cibseven.bpm.engine.ProcessEngine;
import org.cibseven.bpm.engine.RuntimeService;
import org.cibseven.bpm.engine.impl.cfg.AbstractProcessEnginePlugin;
import org.cibseven.bpm.engine.impl.cfg.ProcessEngineConfigurationImpl;
import org.cibseven.bpm.engine.impl.cfg.ProcessEnginePlugin;
import org.cibseven.bpm.engine.impl.history.handler.DbHistoryEventHandler;
import org.cibseven.bpm.engine.impl.history.handler.HistoryEventHandler;
import org.cibseven.bpm.engine.impl.variable.serializer.ByteArrayValueSerializer;
import org.cibseven.bpm.engine.impl.variable.serializer.FileValueSerializer;
import org.cibseven.bpm.engine.impl.variable.serializer.JavaObjectSerializer;
import org.cibseven.bpm.engine.impl.variable.serializer.TypedValueSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;

@AutoConfiguration
@ConditionalOnClass(org.cibseven.bpm.engine.ProcessEngine.class)
@EnableConfigurationProperties({NatsProperties.class, CibSevenNatsProperties.class, A2Properties.class,
        HistoryClassificationProperties.class, HistoryOutboxProperties.class,
        com.threeai.nats.core.vault.PseudonymVaultDataSourceProperties.class,
        LargeVariableExternalizationProperties.class, LargeVariableProjectionDataSourceProperties.class,
        OutboundClassificationProperties.class, OutboundMessageOutboxProperties.class})
public class CibSevenNatsAutoConfiguration {

    private static final String ENGINE_ID = "cibseven";
    private static final String RELAY_LEADER_BUCKET = "history-relay-leader";
    private static final String LARGE_VARIABLE_LEADER_BUCKET = "large-variable-sweep-leader";
    private static final String OUTBOUND_RELAY_LEADER_BUCKET = "outbound-relay-leader";
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
    public JetStreamStreamManager jetStreamStreamManager(NatsProperties props) {
        return new JetStreamStreamManager(props.getJetstream().getStreamReplicas());
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

    /**
     * Startup topology self-check ({@link JetStreamTopologyCheck}): reports broker state that
     * cannot run multi-node — exclusive durables, single-replica streams on a cluster, KV
     * replicas against a non-clustered server — BEFORE any subscription tries to bind. Both
     * registrar beans below take this bean as a constructor dependency purely for ordering: the
     * WARN must print before the [SUB-90012] it predicts, because that failure aborts startup.
     * Subjects are resolved to streams on the live broker; a subject whose stream does not exist
     * yet (first boot, auto-created streams) is skipped and picked up on the next start.
     */
    @Bean
    public NatsTopologySelfCheck natsTopologySelfCheck(Connection connection, NatsProperties natsProperties,
            CibSevenNatsProperties properties, A2Properties a2Properties) {
        java.util.List<JetStreamTopologyCheck.SubjectBinding> bindings = new java.util.ArrayList<>();
        for (String topic : a2Properties.getTopics()) {
            bindings.add(new JetStreamTopologyCheck.SubjectBinding(
                    "jobs." + topic + ".reply", "a2-completion-" + topic));
        }
        if (!a2Properties.getTopics().isEmpty()) {
            bindings.add(new JetStreamTopologyCheck.SubjectBinding("dlq.jobs.>", "a2-incident-bridge"));
        }
        for (com.threeai.nats.cibseven.inbound.SubscriptionConfig subscription : properties.getSubscriptions()) {
            if (subscription.getDurableName() != null && !subscription.getDurableName().isBlank()) {
                bindings.add(new JetStreamTopologyCheck.SubjectBinding(
                        subscription.getSubject(), subscription.getDurableName()));
            }
        }
        return new NatsTopologySelfCheck(connection, bindings, natsProperties.getJetstream().getKvReplicas());
    }

    @Bean
    public NatsSubscriptionRegistrar natsSubscriptionRegistrar(
            CibSevenNatsProperties properties,
            Connection connection,
            JetStream jetStream,
            JetStreamStreamManager streamManager,
            RuntimeService runtimeService,
            @Autowired(required = false) NatsChannelMetrics metrics,
            DlqPublisher dlqPublisher,
            NatsTopologySelfCheck topologySelfCheck, // ordering only: findings print before binds
            @Autowired(required = false) com.threeai.nats.core.shard.ShardTopology shardTopology) {
        NatsSubscriptionRegistrar registrar = new NatsSubscriptionRegistrar(
                properties, connection, jetStream, streamManager, runtimeService, metrics, dlqPublisher);
        registrar.setShardTopology(shardTopology); // null = unsharded, bit-for-bit legacy
        return registrar;
    }

    // --- A2 (increment 1) ---

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
            @Autowired(required = false) NatsChannelMetrics metrics, UmbrellaLockValidator lockValidator,
            @Autowired(required = false) com.threeai.nats.core.shard.ShardTopology shardTopology,
            A2Properties a2Properties) {
        com.threeai.nats.core.jetstream.BoundedAsyncPublisher asyncPublisher =
                a2Properties.isAsyncPublish()
                        ? new com.threeai.nats.core.jetstream.BoundedAsyncPublisher(
                                jetStream, a2Properties.getAsyncPublishMaxInFlight())
                        : null; // kacis kapisi: spring.nats.cibseven.a2.async-publish=false
        A2PostCommitPublisher publisher = new A2PostCommitPublisher(jetStream, metrics,
                lockValidator, asyncPublisher);
        if (shardTopology != null) {
            publisher.setShardTopology(shardTopology); // envelope gains the Reply-Subject address
        }
        return publisher;
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
            UmbrellaLockValidator lockValidator, JetStreamKvManager kvManager, NatsProperties natsProperties,
            NatsTopologySelfCheck topologySelfCheck, // ordering only: findings print before binds
            @Autowired(required = false) com.threeai.nats.core.shard.ShardTopology shardTopology) {
        A2SubscriptionRegistrar registrar = new A2SubscriptionRegistrar(a2Properties, connection, jetStream,
                externalTaskService, dlqPublisher, metrics, meterRegistry, processEngine, lockResolver,
                topicConfig, lockValidator, kvManager, natsProperties.getJetstream().getKvReplicas());
        registrar.setShardTopology(shardTopology); // null = unsharded, bit-for-bit legacy
        return registrar;
    }

    // --- History Offload (increment 2) ---

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
        config.setPoolName("cibseven-pseudonym-vault-pool");
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
    @ConditionalOnProperty(prefix = "spring.nats.cibseven.history", name = "enabled",
            havingValue = "true")
    public CompactHistoryOutboxWriter compactHistoryOutboxWriter(DataSource dataSource,
            PseudonymTokenGenerator pseudonymGenerator, HistoryClassificationProperties classification,
            @Autowired(required = false) NatsChannelMetrics metrics,
            @Autowired(required = false) com.threeai.nats.core.vault.PseudonymizationVaultClient vaultClient) {
        return new CompactHistoryOutboxWriter(dataSource, pseudonymGenerator, classification, metrics, vaultClient);
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "spring.nats.cibseven.history", name = "enabled",
            havingValue = "true")
    public HistoryPostCommitPublisher historyPostCommitPublisher(JetStream jetStream,
            @Autowired(required = false) NatsChannelMetrics metrics,
            HistoryClassificationProperties classificationProperties) {
        return new HistoryPostCommitPublisher(jetStream, metrics,
                classificationProperties.isAsyncPublish()
                        ? new com.threeai.nats.core.jetstream.BoundedAsyncPublisher(
                                jetStream, classificationProperties.getAsyncPublishMaxInFlight())
                        : null); // kacis: spring.nats.cibseven.history.async-publish=false
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
    @ConditionalOnProperty(prefix = "spring.nats.cibseven.history", name = "enabled",
            havingValue = "true")
    public ClassCutoverStateRegistry classCutoverStateRegistry(JetStreamKvManager kvManager, Connection connection,
            NatsProperties natsProperties) {
        kvManager.ensureBucket(CUTOVER_STATE_BUCKET, Duration.ZERO,
                natsProperties.getJetstream().getKvReplicas(), connection);
        ClassCutoverStateRegistry registry = new ClassCutoverStateRegistry(kvManager, connection, ENGINE_ID);
        registry.loadAtBootstrap();
        return registry;
    }

    /** Ensures {@code history-relay-leader} — separate bucket/key namespace from {@code a2-sweep-leader}. */
    @Bean
    @ConditionalOnMissingBean(name = "historyRelayLeaderLease")
    @ConditionalOnBean(DataSource.class)
    @ConditionalOnProperty(prefix = "spring.nats.cibseven.history", name = "enabled",
            havingValue = "true")
    public SweepLeaderLease historyRelayLeaderLease(JetStream jetStream, JetStreamKvManager kvManager,
            Connection connection, HistoryOutboxProperties outboxProperties, NatsProperties natsProperties,
            @Autowired(required = false) com.threeai.nats.core.shard.ShardTopology shardTopology) {
        Duration ttl = Duration.ofSeconds(2 * outboxProperties.getRelayCyclePeriodSeconds());
        kvManager.ensureBucket(RELAY_LEADER_BUCKET, ttl, natsProperties.getJetstream().getKvReplicas(), connection);
        return new SweepLeaderLease(jetStream, kvManager, connection, RELAY_LEADER_BUCKET,
                RELAY_LEADER_KEY_PREFIX, leaseScope(shardTopology), resolveNodeId(), ttl);
    }

    /**
     * docs/13 G4 saha bulgusu (2026-08-24): her DB-basina lider (sweep/relay) sharded modda
     * kendi shard'inin kirasini tutmali — filo-global kira, liderligi baska shard'a tasiyip
     * BU shard'in isini (oksuz supurme, outbox relay) susuz birakir. Kira kimligi yalniz KV
     * anahtarini belirler; subject'lere/engineId'ye SIZMAZ.
     */
    private static String leaseScope(com.threeai.nats.core.shard.ShardTopology shardTopology) {
        return shardTopology != null ? ENGINE_ID + "-s" + shardTopology.getShardId() : ENGINE_ID;
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnBean(DataSource.class)
    @ConditionalOnProperty(prefix = "spring.nats.cibseven.history", name = "enabled",
            havingValue = "true")
    public HistoryOutboxRelay historyOutboxRelay(DataSource dataSource, JetStream jetStream,
            @Qualifier("historyRelayLeaderLease") SweepLeaderLease historyRelayLeaderLease,
            HistoryOutboxProperties outboxProperties,
            @Autowired(required = false) NatsChannelMetrics metrics) {
        return new HistoryOutboxRelay(dataSource, jetStream, historyRelayLeaderLease, outboxProperties, metrics, ENGINE_ID);
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnBean(DataSource.class)
    @ConditionalOnProperty(prefix = "spring.nats.cibseven.history", name = "enabled",
            havingValue = "true")
    public HistoryOutboxRelayScheduler historyOutboxRelayScheduler(HistoryOutboxRelay historyOutboxRelay,
            HistoryOutboxProperties outboxProperties) {
        return new HistoryOutboxRelayScheduler(historyOutboxRelay, outboxProperties.getRelayCyclePeriodSeconds(), ENGINE_ID);
    }

    /** Requires {@link ClassCutoverStateRegistry}, which is itself gated on a DataSource bean. */
    @Bean
    @ConditionalOnBean(DataSource.class)
    @ConditionalOnProperty(prefix = "spring.nats.cibseven.history", name = "enabled",
            havingValue = "true")
    public ProcessEnginePlugin historyProcessEnginePlugin(ClassCutoverStateRegistry cutoverStateRegistry,
            HistoryClassificationProperties classification,
            @Autowired(required = false) CompactHistoryOutboxWriter outboxWriter,
            HistoryPostCommitPublisher postCommitPublisher) {
        return new AbstractProcessEnginePlugin() {
            /**
             * The history level is resolved in {@code initHistoryLevel()}, which runs inside
             * {@code init()} — after every plugin's {@code preInit}. Validating here would read a
             * null level on every single boot, so the check lives in {@code postInit} below; only
             * the handler wiring, which must be in place before {@code init()}, happens here.
             */
            @Override
            public void preInit(ProcessEngineConfigurationImpl configuration) {
                NatsHistoryEventHandler handler = new NatsHistoryEventHandler(cutoverStateRegistry, classification,
                        outboxWriter, postCommitPublisher, new DbHistoryEventHandler(), ENGINE_ID);
                // enableDefaultDbHistoryEventHandler ALWAYS false -- our composite owns its own
                // internalDbDelegate (see NatsHistoryEventHandler's §1.4 note).
                configuration.setEnableDefaultDbHistoryEventHandler(false);
                List<HistoryEventHandler> customHandlers = new ArrayList<>(configuration.getCustomHistoryEventHandlers());
                customHandlers.add(handler);
                configuration.setCustomHistoryEventHandlers(customHandlers);
            }

            /** Runs once {@code initHistoryLevel()} has resolved the level (BA-Q4: WARN-only). */
            @Override
            public void postInit(ProcessEngineConfigurationImpl configuration) {
                HistoryBootstrapValidator.validate(configuration, classification, ENGINE_ID);
            }
        };
    }

    // --- Large Variable Externalization (increment 3) ---

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
        config.setPoolName("cibseven-large-variable-projection-pool");
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
     * (increment 3 design §2.1 evidence: the fork scans this list FIRST and stops at the first match, so no
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
            Connection connection, LargeVariableExternalizationProperties properties,
            NatsProperties natsProperties,
            @Autowired(required = false) com.threeai.nats.core.shard.ShardTopology shardTopology) {
        Duration ttl = Duration.ofSeconds(2 * properties.getSweepCyclePeriodSeconds());
        kvManager.ensureBucket(LARGE_VARIABLE_LEADER_BUCKET, ttl,
                natsProperties.getJetstream().getKvReplicas(), connection);
        return new SweepLeaderLease(jetStream, kvManager, connection, LARGE_VARIABLE_LEADER_BUCKET,
                "sweep-leader.", leaseScope(shardTopology), resolveNodeId(), ttl);
    }

    /** {@code largeVariableSweepLeaderLease}/{@code largeVariablePostCommitExternalizer} bean-NAME
     *  gating (not type — {@code SweepLeaderLease} also has an unrelated {@code
     *  historyRelayLeaderLease} instance in this SAME configuration): only activates when a
     *  projection {@code DataSource} is actually configured (see those beans' own conditions). */
    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnBean(value = DataSource.class, name = {"largeVariableSweepLeaderLease", "largeVariablePostCommitExternalizer"})
    public LargeVariableExternalizationSweep largeVariableExternalizationSweep(DataSource dataSource,
            @Qualifier("largeVariableSweepLeaderLease") SweepLeaderLease largeVariableSweepLeaderLease,
            LargeVariablePostCommitExternalizer largeVariablePostCommitExternalizer,
            ContentAddressedLargePayloadStore largeVariablePayloadStore,
            LargeVariableExternalizationProperties properties) {
        return new LargeVariableExternalizationSweep(dataSource, largeVariableSweepLeaderLease,
                largeVariablePostCommitExternalizer, largeVariablePayloadStore, properties, ENGINE_ID);
    }

    // --- Outbound Handoff (increment 4) ---

    /**
     * ALWAYS available (no {@code @ConditionalOnBean(DataSource.class)}) — unlike {@code
     * CompactHistoryOutboxWriter}, {@code OutboundMessageOutboxWriter} takes the live engine
     * transaction {@link Connection} as a per-call PARAMETER (resolved via {@code
     * Context.getCommandContext()} at {@code notify()} time), not a Spring-managed {@code
     * DataSource} field — so this bean itself never needs one. It is still only USEFUL when a
     * relay drains the table it writes into, which is why {@code natsOutboundPublisher} (below)
     * only wires it when the relay's own {@code DataSource} gate is also satisfied — see that
     * bean's Javadoc CODER-NOTE.
     */
    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "spring.nats.outbound", name = "enabled",
            havingValue = "true")
    public OutboundMessageOutboxWriter outboundMessageOutboxWriter(
            @Autowired(required = false) NatsChannelMetrics metrics) {
        return new OutboundMessageOutboxWriter(metrics);
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "spring.nats.outbound", name = "enabled",
            havingValue = "true")
    public OutboundPostCommitPublisher outboundPostCommitPublisher(JetStream jetStream,
            @Autowired(required = false) NatsChannelMetrics metrics,
            OutboundClassificationProperties outboundProperties) {
        return new OutboundPostCommitPublisher(jetStream, metrics,
                outboundProperties.isAsyncPublish()
                        ? new com.threeai.nats.core.jetstream.BoundedAsyncPublisher(
                                jetStream, outboundProperties.getAsyncPublishMaxInFlight())
                        : null); // kacis: spring.nats.outbound.async-publish=false
    }

    /**
     * D-A' — tenant BPMN references this bean via {@code delegateExpression="${natsOutboundPublisher}"}
     * on a message-throw event or send-task (opt-in: a tenant that never adds the extension element
     * simply never invokes this listener). {@code outboxWriter} is deliberately passed as {@code
     * null} when no engine {@link DataSource} bean is present (CODER-NOTE, class Javadoc) — kept in
     * lock-step with {@link #outboundMessageRelay} so a writer never exists without a relay to
     * drain it (operational hazard avoidance); {@link NatsOutboundPublisher} fails loudly rather
     * than silently downgrading if a CRITICAL-classified type is ever dispatched in that state.
     */
    @Bean
    @ConditionalOnMissingBean(name = "natsOutboundPublisher")
    @ConditionalOnProperty(prefix = "spring.nats.outbound", name = "enabled",
            havingValue = "true")
    public NatsOutboundPublisher natsOutboundPublisher(OutboundClassificationProperties classification,
            OutboundPostCommitPublisher postCommitPublisher,
            @Autowired(required = false) OutboundMessageOutboxWriter outboundMessageOutboxWriter,
            @Autowired(required = false) DataSource dataSource) {
        OutboundMessageOutboxWriter outboxWriter = dataSource != null ? outboundMessageOutboxWriter : null;
        return new NatsOutboundPublisher(classification, outboxWriter, postCommitPublisher, ENGINE_ID);
    }

    /** Ensures {@code outbound-relay-leader} — separate bucket/key namespace from the other leases. */
    @Bean
    @ConditionalOnMissingBean(name = "outboundRelayLeaderLease")
    @ConditionalOnBean(DataSource.class)
    @ConditionalOnProperty(prefix = "spring.nats.outbound", name = "enabled",
            havingValue = "true")
    public SweepLeaderLease outboundRelayLeaderLease(JetStream jetStream, JetStreamKvManager kvManager,
            Connection connection, OutboundMessageOutboxProperties outboxProperties,
            NatsProperties natsProperties,
            @Autowired(required = false) com.threeai.nats.core.shard.ShardTopology shardTopology) {
        Duration ttl = Duration.ofSeconds(2 * outboxProperties.getRelayCyclePeriodSeconds());
        kvManager.ensureBucket(OUTBOUND_RELAY_LEADER_BUCKET, ttl,
                natsProperties.getJetstream().getKvReplicas(), connection);
        return new SweepLeaderLease(jetStream, kvManager, connection, OUTBOUND_RELAY_LEADER_BUCKET,
                RELAY_LEADER_KEY_PREFIX, leaseScope(shardTopology), resolveNodeId(), ttl);
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnBean(DataSource.class)
    @ConditionalOnProperty(prefix = "spring.nats.outbound", name = "enabled",
            havingValue = "true")
    public OutboundMessageRelay outboundMessageRelay(DataSource dataSource, JetStream jetStream,
            @Qualifier("outboundRelayLeaderLease") SweepLeaderLease outboundRelayLeaderLease,
            OutboundMessageOutboxProperties outboxProperties,
            @Autowired(required = false) NatsChannelMetrics metrics) {
        return new OutboundMessageRelay(dataSource, jetStream, outboundRelayLeaderLease, outboxProperties, metrics, ENGINE_ID);
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnBean(DataSource.class)
    @ConditionalOnProperty(prefix = "spring.nats.outbound", name = "enabled",
            havingValue = "true")
    public OutboundMessageRelayScheduler outboundMessageRelayScheduler(OutboundMessageRelay outboundMessageRelay,
            OutboundMessageOutboxProperties outboxProperties) {
        return new OutboundMessageRelayScheduler(outboundMessageRelay, outboxProperties.getRelayCyclePeriodSeconds(), ENGINE_ID);
    }

    private static String resolveNodeId() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            return "node-" + UUID.randomUUID();
        }
    }
}
