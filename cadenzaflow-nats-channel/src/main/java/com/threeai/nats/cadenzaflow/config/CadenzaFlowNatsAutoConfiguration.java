package com.threeai.nats.cadenzaflow.config;

import java.io.IOException;

import com.threeai.nats.cadenzaflow.a2.A2BpmnParseListener;
import com.threeai.nats.cadenzaflow.a2.A2PostCommitPublisher;
import com.threeai.nats.cadenzaflow.a2.A2Properties;
import com.threeai.nats.cadenzaflow.a2.A2SubscriptionRegistrar;
import com.threeai.nats.cadenzaflow.a2.A2TopicConfig;
import com.threeai.nats.cadenzaflow.a2.UmbrellaLockResolver;
import com.threeai.nats.cadenzaflow.a2.UmbrellaLockValidator;
import com.threeai.nats.core.NatsConnectionFactory;
import com.threeai.nats.core.NatsProperties;
import com.threeai.nats.core.config.NatsTransportSecurityGuard;
import com.threeai.nats.core.dlq.DlqPublisher;
import com.threeai.nats.core.jetstream.JetStreamKvManager;
import com.threeai.nats.core.jetstream.JetStreamStreamManager;
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
@EnableConfigurationProperties({NatsProperties.class, CadenzaFlowNatsProperties.class, A2Properties.class})
public class CadenzaFlowNatsAutoConfiguration {

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
}
