package com.threeai.nats.core;

import static net.logstash.logback.argument.StructuredArguments.kv;

import java.io.IOException;

import io.nats.client.Connection;
import io.nats.client.ErrorListener;
import io.nats.client.Nats;
import io.nats.client.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class NatsConnectionFactory {

    private static final Logger log = LoggerFactory.getLogger(NatsConnectionFactory.class);

    private NatsConnectionFactory() {
    }

    public static Connection create(NatsProperties props) throws IOException, InterruptedException {
        Options.Builder builder = new Options.Builder()
                .server(props.getUrl())
                .connectionTimeout(props.getConnectionTimeout())
                .maxReconnects(props.getMaxReconnects())
                .reconnectWait(props.getReconnectWait())
                .connectionListener((conn, event) -> {
                    switch (event) {
                        case CONNECTED -> log.info("NATS connected", kv("host", conn.getServerInfo().getHost()));
                        case RECONNECTED -> log.warn("NATS reconnected", kv("host", conn.getServerInfo().getHost()));
                        case DISCONNECTED -> log.warn("NATS disconnected");
                        case CLOSED -> log.info("NATS connection closed");
                        default -> log.debug("NATS connection event", kv("event", event.name()));
                    }
                })
                .errorListener(new ErrorListener() {
                    @Override
                    public void errorOccurred(Connection conn, String error) {
                        log.error("NATS error", kv("error", error));
                    }

                    @Override
                    public void exceptionOccurred(Connection conn, Exception exp) {
                        log.error("NATS exception", exp);
                    }

                    @Override
                    public void slowConsumerDetected(Connection conn, io.nats.client.Consumer consumer) {
                        log.warn("NATS slow consumer detected", kv("pending_messages", consumer.getPendingMessageCount()));
                    }
                });

        configureAuth(builder, props);

        return Nats.connect(builder.build());
    }

    private static void configureAuth(Options.Builder builder, NatsProperties props) {
        if (props.getCredentialsFile() != null) {
            builder.authHandler(Nats.credentials(props.getCredentialsFile()));
        } else if (props.getNkeyFile() != null) {
            builder.authHandler(Nats.credentials(null, props.getNkeyFile()));
        } else if (props.getToken() != null) {
            builder.token(props.getToken().toCharArray());
        } else if (props.getUsername() != null) {
            builder.userInfo(props.getUsername(), props.getPassword());
        }
    }
}
