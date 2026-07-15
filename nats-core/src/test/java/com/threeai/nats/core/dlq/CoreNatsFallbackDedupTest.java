package com.threeai.nats.core.dlq;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.time.Duration;

import io.nats.client.Connection;
import io.nats.client.JetStreamManagement;
import io.nats.client.Nats;
import io.nats.client.api.RetentionPolicy;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.impl.Headers;
import io.nats.client.support.NatsJetStreamConstants;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * TEST_SPECIFICATIONS.md (b) — characterization test (not a hard gate): does the JetStream
 * {@code duplicate_window} dedup apply to messages published via the CORE-NATS fallback path
 * ({@code connection.publish(subject, headers, data)}, NOT the JetStream {@code publish} API)
 * that {@link DlqPublisher#publish} falls back to when the primary JetStream publish fails
 * (HLD §11 finding #4b)?
 *
 * <p>Both possible outcomes are treated as a PASS — this test documents the answer, it does not
 * assert a "correct" value. See {@link DlqPublisher} class-level note for how the result should
 * be interpreted once known.
 */
@Testcontainers
class CoreNatsFallbackDedupTest {

    @Container
    static GenericContainer<?> natsContainer = new GenericContainer<>("nats:2.10-alpine")
            .withCommand("--jetstream")
            .withExposedPorts(4222);

    private Connection connection;

    @BeforeEach
    void setUp() throws Exception {
        String url = "nats://" + natsContainer.getHost() + ":" + natsContainer.getMappedPort(4222);
        connection = Nats.connect(url);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    void corePublishFallback_duplicateNatsMsgId_dedupBehaviorIsCharacterized() throws Exception {
        JetStreamManagement jsm = connection.jetStreamManagement();
        jsm.addStream(StreamConfiguration.builder()
                .name("DLQ-DEDUP-TEST")
                .subjects("dlq.>")
                .retentionPolicy(RetentionPolicy.Limits)
                .storageType(StorageType.File)
                .duplicateWindow(Duration.ofSeconds(120))
                .build());

        Headers headers = new Headers();
        String msgId = "task-42.dlq";
        headers.add(NatsJetStreamConstants.MSG_ID_HDR, msgId);
        byte[] data = "payload".getBytes(StandardCharsets.UTF_8);

        // Same Nats-Msg-Id, twice, via core-NATS publish (NOT the JetStream publish API) — the
        // exact fallback path DlqPublisher.publish() takes when the primary JetStream publish fails.
        connection.publish("dlq.jobs.order-fulfillment", headers, data);
        connection.publish("dlq.jobs.order-fulfillment", headers, data);
        connection.flush(Duration.ofSeconds(2));

        // Give the stream a moment to ingest both core-NATS publishes.
        Thread.sleep(500);

        long msgCount = jsm.getStreamInfo("DLQ-DEDUP-TEST").getStreamState().getMsgCount();

        // Both outcomes are a PASS (characterization, not a hard gate) — see class Javadoc.
        assertThat(msgCount).isIn(1L, 2L);
        if (msgCount == 1L) {
            System.out.println("CoreNatsFallbackDedupTest: dedup APPLIES to core-NATS fallback publishes "
                    + "(duplicate_window honored even outside the JetStream publish API).");
        } else {
            System.out.println("CoreNatsFallbackDedupTest: dedup DOES NOT apply to core-NATS fallback "
                    + "publishes — DlqPublisher's PUBLISHED_CORE_FALLBACK outcome can produce duplicate "
                    + "DLQ entries; this is a documented, accepted limitation (03_classes/1_nats_core_common.md §2.3).");
        }
    }
}
