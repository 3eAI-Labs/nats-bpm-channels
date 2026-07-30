package com.threeai.nats.core;

import java.util.Map;

import io.nats.client.Message;
import io.nats.client.impl.Headers;

public final class NatsHeaderUtils {

    private NatsHeaderUtils() {
    }

    public static Headers toNatsHeaders(Map<String, Object> headerMap) {
        if (headerMap == null || headerMap.isEmpty()) {
            return null;
        }
        Headers headers = new Headers();
        headerMap.forEach((key, value) -> {
            if (value != null) {
                headers.add(key, value.toString());
            }
        });
        return headers;
    }

    public static String extractHeader(Message msg, String key) {
        if (msg.getHeaders() != null && msg.getHeaders().containsKey(key)) {
            return msg.getHeaders().getLast(key);
        }
        return null;
    }
}
