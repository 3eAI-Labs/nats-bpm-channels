package com.threeai.nats.camunda.eventing;

import com.threeai.nats.camunda.inbound.JetStreamMessageCorrelationSubscriber;
import com.threeai.nats.camunda.inbound.NatsMessageCorrelationSubscriber;
import com.threeai.nats.camunda.inbound.SubscriptionConfig;
import com.threeai.nats.core.dlq.DlqPublisher;
import com.threeai.nats.core.jetstream.JetStreamStreamManager;
import com.threeai.nats.core.metrics.NatsChannelMetrics;

import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamManagement;
import io.nats.client.api.ConsumerInfo;
import org.camunda.bpm.engine.RuntimeService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Production {@link SubscriberFactory}: builds the slice-5 definition-aware correlation
 * subscribers — the same classes the legacy YAML registrar uses, constructed with the
 * {@link EventDefinition} so payloads map through the type dictionary and correlation
 * parameters become {@code processInstanceVariableEquals} filters.
 */
public class DefinitionSubscriberFactory implements SubscriberFactory {

    private static final Logger log = LoggerFactory.getLogger(DefinitionSubscriberFactory.class);

    private final Connection connection;
    private final JetStream jetStream;
    private final JetStreamStreamManager streamManager;
    private final RuntimeService runtimeService;
    private final NatsChannelMetrics metrics;
    private final DlqPublisher dlqPublisher;

    public DefinitionSubscriberFactory(Connection connection, JetStream jetStream,
            JetStreamStreamManager streamManager, RuntimeService runtimeService,
            NatsChannelMetrics metrics, DlqPublisher dlqPublisher) {
        this.connection = connection;
        this.jetStream = jetStream;
        this.streamManager = streamManager;
        this.runtimeService = runtimeService;
        this.metrics = metrics;
        this.dlqPublisher = dlqPublisher;
    }

    @Override
    public SubscriberHandle subscribe(SubscriptionConfig config, EventDefinition event) {
        if (config.isJetstream()) {
            if (config.isAutoCreateStream() && config.getStreamName() != null) {
                streamManager.ensureStream(config.getStreamName(), config.getSubject(), connection);
            }
            reconcileDurableConfig(config);
            JetStreamMessageCorrelationSubscriber subscriber = new JetStreamMessageCorrelationSubscriber(
                    connection, jetStream, runtimeService, config, metrics, dlqPublisher, event);
            subscriber.subscribe();
            return subscriber::unsubscribe;
        }
        NatsMessageCorrelationSubscriber subscriber = new NatsMessageCorrelationSubscriber(
                connection, runtimeService, config, metrics, event);
        subscriber.subscribe();
        return subscriber::unsubscribe;
    }

    /**
     * Definition redeploys are authoritative (docs/12 G3): if the durable already exists with
     * a DIFFERENT consumer configuration than this definition requests, the server would
     * reject the re-bind — so the stale durable is deleted and re-created. This resets the
     * consumer's delivery state (redelivery counts, pending) — part of the documented
     * unregister→register redeploy window. A matching durable (the restart case) is left
     * untouched and re-bound with its state intact.
     */
    private void reconcileDurableConfig(SubscriptionConfig config) {
        if (config.getDurableName() == null) {
            return;
        }
        try {
            JetStreamManagement jsm = connection.jetStreamManagement();
            String stream = config.getStreamName() != null ? config.getStreamName()
                    : jsm.getStreamNames(config.getSubject()).stream().findFirst().orElse(null);
            if (stream == null) {
                return;
            }
            ConsumerInfo info = jsm.getConsumerInfo(stream, config.getDurableName());
            long existingMaxDeliver = info.getConsumerConfiguration().getMaxDeliver();
            String existingGroup = info.getConsumerConfiguration().getDeliverGroup();
            // mirror of the subscriber's ConsumerConfiguration: maxDeliver+1, resolveDeliverGroup
            boolean matches = existingMaxDeliver == config.getMaxDeliver() + 1L
                    && java.util.Objects.equals(existingGroup, config.resolveDeliverGroup());
            if (!matches) {
                jsm.deleteConsumer(stream, config.getDurableName());
                log.info("Stale durable consumer deleted for definition redeploy —"
                        + " it will be re-created with the definition's configuration"
                        + " (delivery state resets; docs/12 G3 redeploy window)");
            }
        } catch (Exception e) {
            // absent consumer, or lookup failed: let the subscriber's own subscribe decide
            log.debug("Durable pre-check skipped", e);
        }
    }
}
