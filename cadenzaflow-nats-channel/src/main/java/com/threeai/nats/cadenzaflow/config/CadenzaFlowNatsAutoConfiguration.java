package com.threeai.nats.cadenzaflow.config;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import javax.sql.DataSource;

import com.threeai.nats.cadenzaflow.a2.A2BpmnParseListener;
import com.threeai.nats.cadenzaflow.a2.A2PostCommitPublisher;
import com.threeai.nats.cadenzaflow.a2.A2Properties;
import com.threeai.nats.cadenzaflow.a2.A2SubscriptionRegistrar;
import com.threeai.nats.cadenzaflow.a2.A2TopicConfig;
import com.threeai.nats.cadenzaflow.a2.UmbrellaLockResolver;
import com.threeai.nats.cadenzaflow.a2.UmbrellaLockValidator;
import com.threeai.nats.cadenzaflow.history.ClassCutoverStateRegistry;
import com.threeai.nats.cadenzaflow.history.CompactHistoryOutboxWriter;
import com.threeai.nats.cadenzaflow.history.HistoryBootstrapValidator;
import com.threeai.nats.cadenzaflow.history.HistoryClassificationProperties;
import com.threeai.nats.cadenzaflow.history.HistoryOutboxProperties;
import com.threeai.nats.cadenzaflow.history.HistoryOutboxRelay;
import com.threeai.nats.cadenzaflow.history.HistoryOutboxRelayScheduler;
import com.threeai.nats.cadenzaflow.history.HistoryPostCommitPublisher;
import com.threeai.nats.cadenzaflow.history.NatsHistoryEventHandler;
import com.threeai.nats.core.NatsConnectionFactory;
import com.threeai.nats.core.NatsProperties;
import com.threeai.nats.core.config.NatsTransportSecurityGuard;
import com.threeai.nats.core.dlq.DlqPublisher;
import com.threeai.nats.core.history.PseudonymTokenGenerator;
import com.threeai.nats.core.jetstream.JetStreamKvManager;
import com.threeai.nats.core.jetstream.JetStreamStreamManager;
import com.threeai.nats.core.jetstream.SweepLeaderLease;
import com.threeai.nats.core.metrics.NatsChannelMetrics;
import io.micrometer.core.instrument.MeterRegistry;
import io.nats.client.Connection;
import io.nats.client.JetStream;
import org.cadenzaflow.bpm.engine.ExternalTaskService;
import org.cadenzaflow.bpm.engine.ProcessEngine;
import org.cadenzaflow.bpm.engine.RuntimeService;
import org.cadenzaflow.bpm.engine.impl.cfg.AbstractProcessEnginePlugin;
import org.cadenzaflow.bpm.engine.impl.cfg.ProcessEngineConfigurationImpl;
import org.cadenzaflow.bpm.engine.impl.cfg.ProcessEnginePlugin;
import org.cadenzaflow.bpm.engine.impl.history.handler.DbHistoryEventHandler;
import org.cadenzaflow.bpm.engine.impl.history.handler.HistoryEventHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;

@AutoConfiguration
@ConditionalOnClass(org.cadenzaflow.bpm.engine.ProcessEngine.class)
@EnableConfigurationProperties({NatsProperties.class, CadenzaFlowNatsProperties.class, A2Properties.class,
        HistoryClassificationProperties.class, HistoryOutboxProperties.class})
public class CadenzaFlowNatsAutoConfiguration {

    private static final String ENGINE_ID = "cadenzaflow";
    private static final String RELAY_LEADER_BUCKET = "history-relay-leader";
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
            CadenzaFlowNatsProperties properties,
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

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnBean(DataSource.class)
    public CompactHistoryOutboxWriter compactHistoryOutboxWriter(DataSource dataSource,
            PseudonymTokenGenerator pseudonymGenerator, HistoryClassificationProperties classification,
            @Autowired(required = false) NatsChannelMetrics metrics) {
        return new CompactHistoryOutboxWriter(dataSource, pseudonymGenerator, classification, metrics);
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

    private static String resolveNodeId() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            return "node-" + UUID.randomUUID();
        }
    }
}
