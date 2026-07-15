package org.flowable.eventregistry.spring.nats.config;

import java.io.IOException;

import com.threeai.nats.core.NatsConnectionFactory;
import com.threeai.nats.core.NatsProperties;
import com.threeai.nats.core.config.NatsTransportSecurityGuard;
import com.threeai.nats.core.dlq.DlqPublisher;
import com.threeai.nats.core.jetstream.JetStreamStreamManager;
import com.threeai.nats.core.metrics.NatsChannelMetrics;
import com.threeai.nats.core.resilience.DlqBridgeCircuitBreakerFactory;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.micrometer.core.instrument.MeterRegistry;
import io.nats.client.Connection;
import io.nats.client.JetStream;
import org.flowable.eventregistry.api.EventRegistry;
import org.flowable.eventregistry.spring.nats.NatsChannelDefinitionProcessor;
import org.flowable.eventregistry.spring.nats.escalation.FailureEventBridge;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;

@AutoConfiguration
@ConditionalOnClass({ Connection.class, EventRegistry.class })
@EnableConfigurationProperties(NatsProperties.class)
public class FlowableNatsAutoConfiguration {

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
    @ConditionalOnMissingBean
    public NatsChannelDefinitionProcessor natsChannelDefinitionProcessor(
            Connection connection,
            JetStream jetStream,
            JetStreamStreamManager streamManager,
            @Autowired(required = false) NatsChannelMetrics metrics,
            DlqPublisher dlqPublisher) {
        return new NatsChannelDefinitionProcessor(connection, jetStream, streamManager, metrics, dlqPublisher);
    }

    @Bean(initMethod = "subscribe", destroyMethod = "unsubscribe")
    @ConditionalOnMissingBean
    public FailureEventBridge failureEventBridge(Connection connection, JetStream jetStream,
            EventRegistry eventRegistry, NatsChannelDefinitionProcessor channelModelLookup,
            @Autowired(required = false) NatsChannelMetrics metrics,
            @Autowired(required = false) MeterRegistry meterRegistry) {
        CircuitBreaker circuitBreaker = DlqBridgeCircuitBreakerFactory.create(
                "cb-failure-event-bridge-flowable", meterRegistry);
        return new FailureEventBridge(connection, jetStream, "dlq.>", eventRegistry, channelModelLookup,
                circuitBreaker, metrics);
    }
}
