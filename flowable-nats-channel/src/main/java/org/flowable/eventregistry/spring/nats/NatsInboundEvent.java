package org.flowable.eventregistry.spring.nats;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import io.nats.client.Message;
import org.flowable.eventregistry.api.InboundEvent;

public class NatsInboundEvent implements InboundEvent {

    private final Message rawMessage;
    private final String body;
    private final Map<String, Object> headers;

    public NatsInboundEvent(Message message) {
        this.rawMessage = message;
        this.body = message.getData() != null
                ? new String(message.getData(), StandardCharsets.UTF_8)
                : null;
        this.headers = extractHeaders(message);
    }

    @Override
    public Object getRawEvent() {
        return rawMessage;
    }

    @Override
    public Object getBody() {
        return body;
    }

    @Override
    public Map<String, Object> getHeaders() {
        return headers;
    }

    private static Map<String, Object> extractHeaders(Message message) {
        if (message.getHeaders() == null || message.getHeaders().isEmpty()) {
            return Collections.emptyMap();
        }
        Map<String, Object> headers = new HashMap<>();
        for (String key : message.getHeaders().keySet()) {
            headers.put(key, message.getHeaders().getLast(key));
        }
        return Collections.unmodifiableMap(headers);
    }
}
