package com.threeai.nats.flowable.externalworker.config;

import java.util.ArrayList;
import java.util.List;

import com.threeai.nats.core.dlq.DlqPublisher;
import com.threeai.nats.core.jetstream.JetStreamKvManager;
import com.threeai.nats.core.jetstream.JetStreamTopologyCheck;
import com.threeai.nats.core.jetstream.NatsTopologySelfCheck;
import com.threeai.nats.core.metrics.NatsChannelMetrics;
import com.threeai.nats.flowable.externalworker.EwLockConfig;
import com.threeai.nats.flowable.externalworker.EwProperties;
import com.threeai.nats.flowable.externalworker.EwSubscriptionRegistrar;
import com.threeai.nats.flowable.externalworker.EwTopicConfig;
import io.nats.client.Connection;
import io.nats.client.JetStream;
import org.flowable.engine.ProcessEngine;
import org.flowable.eventregistry.spring.nats.config.FlowableNatsAutoConfiguration;
import com.threeai.nats.core.NatsConnectionFactory;
import com.threeai.nats.core.NatsProperties;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

/**
 * Auto-configuration of the external-worker dispatch capability (docs/11 D-F', R-yellow-10a):
 * a SEPARATE class from the Event Registry adapter's {@link FlowableNatsAutoConfiguration},
 * conditional on {@link ProcessEngine} instead of {@code EventRegistry} — the capability works
 * with or without the Event Registry on the classpath. Infra beans (connection, JetStream, DLQ
 * publisher) are {@code @ConditionalOnMissingBean} fallbacks over the same factories, so
 * whichever auto-configuration runs first owns them.
 *
 * <p>The self-check bean is NAMED ({@code ewNatsTopologySelfCheck}) and the Event Registry
 * bridge's injection point is qualifier-pinned to its own bean — two unnamed beans of the same
 * type would kill the context (N-yellow-3).
 */
@AutoConfiguration(after = FlowableNatsAutoConfiguration.class)
@ConditionalOnClass({ Connection.class, ProcessEngine.class })
@ConditionalOnProperty(prefix = "spring.nats.flowable.external-worker", name = "enabled", havingValue = "true")
@EnableConfigurationProperties({ EwProperties.class, NatsProperties.class })
public class FlowableExternalWorkerAutoConfiguration {

    @Bean(destroyMethod = "close")
    @ConditionalOnMissingBean
    public Connection natsConnection(NatsProperties props) throws Exception {
        return NatsConnectionFactory.create(props);
    }

    @Bean
    @ConditionalOnMissingBean
    public JetStream natsJetStream(Connection connection) throws Exception {
        return connection.jetStream();
    }

    @Bean
    @ConditionalOnMissingBean
    public JetStreamKvManager jetStreamKvManager() {
        return new JetStreamKvManager();
    }

    @Bean
    @ConditionalOnMissingBean
    public DlqPublisher dlqPublisher(JetStream jetStream, Connection connection,
            @Autowired(required = false) NatsChannelMetrics metrics) {
        return new DlqPublisher(jetStream, connection, metrics);
    }

    @Bean
    public EwTopicConfig ewTopicConfig(EwProperties properties) {
        return new EwTopicConfig(properties);
    }

    @Bean
    public EwLockConfig ewLockConfig(EwProperties properties) {
        return new EwLockConfig(properties);
    }

    /**
     * Boot-time topology audit for the EW bindings (gate G3): reply durables and the incident
     * consumer's wildcard, WARNed against the live broker before any bind would fail.
     */
    @Bean("ewNatsTopologySelfCheck")
    public NatsTopologySelfCheck ewNatsTopologySelfCheck(Connection connection,
            EwProperties ewProperties, EwTopicConfig topicConfig, NatsProperties natsProperties) {
        List<JetStreamTopologyCheck.SubjectBinding> bindings = new ArrayList<>();
        for (String topic : topicConfig.topics()) {
            bindings.add(new JetStreamTopologyCheck.SubjectBinding(
                    "ewjobs." + topic + ".reply", "flw-ew-completion-" + topic));
        }
        bindings.add(new JetStreamTopologyCheck.SubjectBinding("dlq.ewjobs.>", "flw-ew-incident"));
        return new NatsTopologySelfCheck(connection, bindings,
                natsProperties.getJetstream().getKvReplicas());
    }

    @Bean(initMethod = "start", destroyMethod = "stop")
    @ConditionalOnBean(ProcessEngine.class)
    public EwSubscriptionRegistrar ewSubscriptionRegistrar(Connection connection,
            JetStream jetStream, JetStreamKvManager kvManager, ProcessEngine processEngine,
            EwProperties properties, EwTopicConfig topicConfig, EwLockConfig lockConfig,
            DlqPublisher dlqPublisher, @Autowired(required = false) NatsChannelMetrics metrics,
            NatsProperties natsProperties,
            @Qualifier("ewNatsTopologySelfCheck") NatsTopologySelfCheck selfCheck) {
        // selfCheck parameter is ordering-only: findings print before the binds they predict.
        return new EwSubscriptionRegistrar(connection, jetStream, kvManager, processEngine,
                properties, topicConfig, lockConfig, dlqPublisher, metrics,
                natsProperties.getJetstream().getKvReplicas());
    }
}
