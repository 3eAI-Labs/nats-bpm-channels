package com.threeai.nats.history.projection;

import static net.logstash.logback.argument.StructuredArguments.kv;

import java.time.Duration;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.JetStream;
import io.nats.client.PushSubscribeOptions;
import io.nats.client.api.ConsumerConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;

/**
 * Subscribes {@link HistoryDlqInspectionConsumer} to the wildcard {@code dlq.history.>} subject
 * (`03_classes/2_relay_projection.md` §4, ops-only visibility — no DLQ-of-DLQ per the asyncapi
 * {@code historyDeadLetter} contract).
 *
 * <p><b>CODER-NOTE (maxDeliver=5):</b> the asyncapi contract fixes {@code historyEvent}'s
 * {@code x-jetstream.maxDeliver=4} but does NOT specify a value for the {@code historyDeadLetter}
 * channel itself (there is no further escalation target once a message is already in the DLQ
 * stream). This mirrors basamak-1's {@code A2ConsumerConfig} plain message-correlation default
 * (5, i.e. {@code maxDeliver+1} below) rather than inventing an unrelated number.
 */
public class HistoryDlqInspectionSubscriptionRegistrar implements InitializingBean, DisposableBean {

    private static final Logger log = LoggerFactory.getLogger(HistoryDlqInspectionSubscriptionRegistrar.class);
    private static final String DLQ_HISTORY_WILDCARD_SUBJECT = "dlq.history.>";
    private static final String DURABLE_NAME = "history-dlq-inspection";
    private static final int MAX_DELIVER = 4;

    private final Connection connection;
    private final JetStream jetStream;
    private final HistoryDlqInspectionConsumer consumer;

    private Dispatcher dispatcher;

    public HistoryDlqInspectionSubscriptionRegistrar(Connection connection, JetStream jetStream,
            HistoryDlqInspectionConsumer consumer) {
        this.connection = connection;
        this.jetStream = jetStream;
        this.consumer = consumer;
    }

    @Override
    public void afterPropertiesSet() {
        try {
            dispatcher = connection.createDispatcher();
            ConsumerConfiguration cc = ConsumerConfiguration.builder()
                    .durable(DURABLE_NAME)
                    .ackWait(Duration.ofSeconds(30))
                    .maxDeliver(MAX_DELIVER + 1)
                    .build();
            PushSubscribeOptions opts = PushSubscribeOptions.builder().configuration(cc).build();
            jetStream.subscribe(DLQ_HISTORY_WILDCARD_SUBJECT, dispatcher, consumer::onMessage, false, opts);
            log.info("Registered history DLQ inspection subscription", kv("subject", DLQ_HISTORY_WILDCARD_SUBJECT));
        } catch (Exception e) {
            throw new IllegalStateException("Failed to subscribe history DLQ inspection consumer", e);
        }
    }

    @Override
    public void destroy() {
        if (dispatcher != null) {
            try {
                dispatcher.drain(Duration.ofSeconds(10));
            } catch (Exception e) {
                log.warn("Error draining history DLQ inspection dispatcher", e);
            }
        }
    }
}
