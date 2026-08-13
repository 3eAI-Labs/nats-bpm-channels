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
import org.flowable.eventregistry.impl.EventRegistryEngineConfiguration;
import org.flowable.eventregistry.json.converter.ChannelJsonConverter;
import org.flowable.eventregistry.spring.nats.channel.NatsInboundChannelModel;
import org.flowable.eventregistry.spring.nats.channel.NatsOutboundChannelModel;
import org.flowable.eventregistry.spring.nats.NatsChannelDefinitionProcessor;
import org.flowable.eventregistry.spring.nats.escalation.FailureEventBridge;
import org.springframework.beans.factory.InitializingBean;
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

    /** The {@code type} discriminator in a Flowable channel definition. */
    private static final String CHANNEL_TYPE = "nats";

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
     * Teaches Flowable's {@code ChannelJsonConverter} that {@code "type": "nats"} means our channel
     * models. Without this the documented channel definition does not deploy at all — the converter
     * looks up {@code <channelType>-<type>} in its own map and throws
     * {@code FlowableEventJsonException: Not supported inbound channel model type was found nats},
     * because {@code addDefaultChannelModelClasses()} only knows jms, rabbit, kafka, camel and
     * expression.
     *
     * <p>Every test in this module builds {@link NatsInboundChannelModel} with {@code new}, so the
     * deployment path — the only one a user has — was never exercised. Registered here as an
     * {@link InitializingBean} so it happens while the engine configuration is being built, before
     * any {@code .channel} resource is deployed.
     */
    @Bean
    @ConditionalOnBean(EventRegistryEngineConfiguration.class)
    public InitializingBean natsChannelModelTypeRegistrar(
            EventRegistryEngineConfiguration eventRegistryEngineConfiguration) {
        return () -> {
            ChannelJsonConverter converter = eventRegistryEngineConfiguration.getChannelJsonConverter();
            if (converter == null) {
                return;
            }
            converter.addInboundChannelModelClass(CHANNEL_TYPE, NatsInboundChannelModel.class);
            converter.addOutboundChannelModelClass(CHANNEL_TYPE, NatsOutboundChannelModel.class);
        };
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
            @Autowired(required = false) MeterRegistry meterRegistry,
            @Autowired(required = false) EventRegistryEngineConfiguration eventRegistryEngineConfiguration) {
        // CB benign-exception clarification (review MAJOR-1a placeholder, resolved 2026-07-15 by
        // QA test (d) / EventReceivedNoMatchBehaviorTest): eventReceived(...)
        // does NOT throw on "no waiting subscription" — it returns silently, so there is no
        // benign-but-not-downstream-health exception type analogous to A2IncidentBridge's
        // NotFoundException for THIS bridge.
        // ignoreExceptions(...) intentionally stays empty — not an oversight.
        CircuitBreaker circuitBreaker = DlqBridgeCircuitBreakerFactory.create(
                "cb-failure-event-bridge-flowable", meterRegistry);
        return new FailureEventBridge(connection, jetStream, "dlq.>", eventRegistry, channelModelLookup,
                circuitBreaker, metrics, eventRegistryEngineConfiguration);
    }
}
