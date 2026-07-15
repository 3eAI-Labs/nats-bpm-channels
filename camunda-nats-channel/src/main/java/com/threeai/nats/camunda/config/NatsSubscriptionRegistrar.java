package com.threeai.nats.camunda.config;

import static net.logstash.logback.argument.StructuredArguments.kv;

import java.util.ArrayList;
import java.util.List;

import com.threeai.nats.camunda.inbound.JetStreamMessageCorrelationSubscriber;
import com.threeai.nats.camunda.inbound.NatsMessageCorrelationSubscriber;
import com.threeai.nats.camunda.inbound.SubscriptionConfig;
import com.threeai.nats.core.dlq.DlqPublisher;
import com.threeai.nats.core.jetstream.JetStreamStreamManager;
import com.threeai.nats.core.metrics.NatsChannelMetrics;
import io.nats.client.Connection;
import io.nats.client.JetStream;
import org.camunda.bpm.engine.RuntimeService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NatsSubscriptionRegistrar implements
        org.springframework.beans.factory.InitializingBean,
        org.springframework.beans.factory.DisposableBean {

    private static final Logger log = LoggerFactory.getLogger(NatsSubscriptionRegistrar.class);

    private final CamundaNatsProperties properties;
    private final Connection connection;
    private final JetStream jetStream;
    private final JetStreamStreamManager streamManager;
    private final RuntimeService runtimeService;
    private final NatsChannelMetrics metrics;
    private final DlqPublisher dlqPublisher;

    private final List<NatsMessageCorrelationSubscriber> coreSubscribers = new ArrayList<>();
    private final List<JetStreamMessageCorrelationSubscriber> jsSubscribers = new ArrayList<>();

    public NatsSubscriptionRegistrar(CamundaNatsProperties properties,
            Connection connection, JetStream jetStream,
            JetStreamStreamManager streamManager,
            RuntimeService runtimeService, NatsChannelMetrics metrics, DlqPublisher dlqPublisher) {
        this.properties = properties;
        this.connection = connection;
        this.jetStream = jetStream;
        this.streamManager = streamManager;
        this.runtimeService = runtimeService;
        this.metrics = metrics;
        this.dlqPublisher = dlqPublisher;
    }

    @Override
    public void afterPropertiesSet() {
        for (SubscriptionConfig config : properties.getSubscriptions()) {
            if (config.isJetstream()) {
                if (config.isAutoCreateStream() && config.getStreamName() != null) {
                    streamManager.ensureStream(config.getStreamName(), config.getSubject(), connection);
                }
                JetStreamMessageCorrelationSubscriber subscriber =
                        new JetStreamMessageCorrelationSubscriber(
                                connection, jetStream, runtimeService, config, metrics, dlqPublisher);
                subscriber.subscribe();
                jsSubscribers.add(subscriber);
                log.info("Registered JetStream Camunda subscription",
                        kv("subject", config.getSubject()),
                        kv("message_name", config.getMessageName()));
            } else {
                NatsMessageCorrelationSubscriber subscriber =
                        new NatsMessageCorrelationSubscriber(
                                connection, runtimeService, config, metrics);
                subscriber.subscribe();
                coreSubscribers.add(subscriber);
                log.info("Registered Core NATS Camunda subscription",
                        kv("subject", config.getSubject()),
                        kv("message_name", config.getMessageName()));
            }
        }
    }

    @Override
    public void destroy() {
        coreSubscribers.forEach(NatsMessageCorrelationSubscriber::unsubscribe);
        jsSubscribers.forEach(JetStreamMessageCorrelationSubscriber::unsubscribe);
        log.info("All Camunda NATS subscriptions unsubscribed");
    }
}
