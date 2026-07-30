package org.flowable.eventregistry.spring.nats;

import static net.logstash.logback.argument.StructuredArguments.kv;

import java.time.Duration;

import com.threeai.nats.core.NatsHeaderUtils;
import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Message;
import org.flowable.eventregistry.api.EventRegistry;
import org.flowable.eventregistry.api.InboundEventChannelAdapter;
import org.flowable.eventregistry.model.InboundChannelModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

public class NatsInboundEventChannelAdapter implements InboundEventChannelAdapter {

    private static final Logger log = LoggerFactory.getLogger(NatsInboundEventChannelAdapter.class);

    private final Connection connection;
    private final String subject;
    private final String queueGroup;

    private InboundChannelModel inboundChannelModel;
    private EventRegistry eventRegistry;
    private Dispatcher dispatcher;

    public NatsInboundEventChannelAdapter(Connection connection, String subject, String queueGroup) {
        this.connection = connection;
        this.subject = subject;
        this.queueGroup = queueGroup;
    }

    @Override
    public void setInboundChannelModel(InboundChannelModel inboundChannelModel) {
        this.inboundChannelModel = inboundChannelModel;
    }

    @Override
    public void setEventRegistry(EventRegistry eventRegistry) {
        this.eventRegistry = eventRegistry;
    }

    public void subscribe() {
        this.dispatcher = connection.createDispatcher();
        if (queueGroup != null && !queueGroup.isBlank()) {
            dispatcher.subscribe(subject, queueGroup, this::handleMessage);
        } else {
            dispatcher.subscribe(subject, this::handleMessage);
        }
        log.info("Subscribed to NATS subject",
                kv("channel", inboundChannelModel.getKey()),
                kv("subject", subject),
                kv("queue_group", queueGroup));
    }

    public void unsubscribe() {
        if (dispatcher != null) {
            try {
                dispatcher.drain(Duration.ofSeconds(5));
            } catch (Exception e) {
                log.warn("Error draining dispatcher",
                        kv("channel", inboundChannelModel.getKey()), e);
            }
            connection.closeDispatcher(dispatcher);
            dispatcher = null;
            log.info("Unsubscribed from NATS subject",
                    kv("channel", inboundChannelModel.getKey()),
                    kv("subject", subject));
        }
    }

    void handleMessage(Message message) {
        String traceId = NatsHeaderUtils.extractHeader(message, "X-Trace-Id");
        try {
            if (traceId != null) {
                MDC.put("trace_id", traceId);
            }
            if (message.getData() == null || message.getData().length == 0) {
                log.warn("Empty message received, skipping",
                        kv("channel", inboundChannelModel.getKey()),
                        kv("subject", message.getSubject()));
                return;
            }
            NatsInboundEvent event = new NatsInboundEvent(message);
            eventRegistry.eventReceived(inboundChannelModel, event);
        } catch (Exception e) {
            log.error("Message processing failed",
                    kv("channel", inboundChannelModel.getKey()),
                    kv("subject", message.getSubject()), e);
        } finally {
            MDC.remove("trace_id");
        }
    }
}
