package com.threeai.nats.core.jetstream;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.time.Duration;

import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamManagement;
import io.nats.client.Nats;
import io.nats.client.api.RetentionPolicy;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.impl.Headers;
import io.nats.client.impl.NatsMessage;
import io.nats.client.support.NatsJetStreamConstants;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Sentinel Phase 5.5 (QA) — regression/characterization test for the deployment-topology hazard
 * the coder found empirically while building {@code nats-bpm-bench}
 * ({@code BenchEnvironment.ensureStreams} CODER-NOTE, {@code nats-bpm-bench/.../BenchEnvironment
 * .java:171-183}, "CODER-Q6" in the phase-5 summary):
 *
 * <p><b>The hazard:</b> {@code jobs.<topic>} (job dispatch) and {@code jobs.<topic>.reply} (worker
 * reply) intentionally carry the SAME {@code Nats-Msg-Id} (= externalTaskId, IR-3/asyncapi
 * {@code ReplyHeaders}). JetStream deduplication via {@code duplicate_window} is
 * <b>stream-scoped, not subject-scoped</b> — if an operator ever provisions ONE stream spanning
 * both subjects (a plausible simplification, since {@code jobs.*} is documented as a single
 * reserved namespace, BAQ-4/BR-SUB-004), JetStream treats the reply as a duplicate of its own job
 * dispatch and <b>silently drops it</b> — the worker's reply never reaches
 * {@code A2CompletionBridge}, and the external task hangs until the umbrella-lock sweep (up to
 * {@code L} seconds later) re-publishes the job, masking the real defect as a "slow worker".
 *
 * <p>This test proves the hazard is real (test 1) and proves the documented-safe topology
 * (separate streams per subject, matching {@code nats-bpm-bench}'s {@code ensureStreams} and the
 * asyncapi per-channel {@code x-jetstream} blocks) avoids it (test 2) — a concrete regression
 * guard for the deployment runbook recommendation, not just prose.
 */
@Testcontainers
class JobReplySameStreamDedupRegressionTest {

    // Distinct topics per test method — the NATS container (and its subject namespace) is shared
    // across both @Test methods in this class (Testcontainers @Container static field), so the
    // two scenarios must not declare streams over overlapping subjects.
    private static final String UNSAFE_TOPIC = "order-fulfillment-unsafe";
    private static final String SAFE_TOPIC = "order-fulfillment-safe";

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
    void unsafeTopology_oneStreamSpanningJobAndReply_replyIsSilentlyDeduped() throws Exception {
        String jobSubject = "jobs." + UNSAFE_TOPIC;
        String replySubject = "jobs." + UNSAFE_TOPIC + ".reply";

        JetStreamManagement jsm = connection.jetStreamManagement();
        jsm.addStream(StreamConfiguration.builder()
                .name("UNSAFE-COMBINED-STREAM")
                .subjects(jobSubject, replySubject) // <-- the misconfiguration under test
                .retentionPolicy(RetentionPolicy.Limits)
                .storageType(StorageType.File)
                .duplicateWindow(Duration.ofSeconds(120))
                .build());

        JetStream jetStream = connection.jetStream();
        String externalTaskId = "task-collision-1";

        // 1) Job dispatch (A2PostCommitPublisher analogue): Nats-Msg-Id = externalTaskId.
        publish(jetStream, jobSubject, externalTaskId, "job-payload");
        // 2) Worker reply (same externalTaskId, per IR-3/asyncapi ReplyHeaders contract) arrives
        //    within the SAME duplicate_window, on a DIFFERENT subject but the SAME stream.
        publish(jetStream, replySubject, externalTaskId, "reply-payload");

        connection.flush(Duration.ofSeconds(2));
        Thread.sleep(500);

        long msgCount = jsm.getStreamInfo("UNSAFE-COMBINED-STREAM").getStreamState().getMsgCount();

        // Only the job survives — the reply was deduped away because dedup is stream-scoped, not
        // subject-scoped. This is the danger the coder found empirically; this assertion is a
        // hard gate (unlike the (b) core-publish characterization test) precisely BECAUSE it
        // reproduces a real defect, not an open/ambiguous design question.
        assertThat(msgCount)
                .as("JetStream dedup is stream-scoped: a reply sharing its job's Nats-Msg-Id on a "
                        + "combined stream is silently dropped, never reaching A2CompletionBridge")
                .isEqualTo(1L);
    }

    @Test
    void safeTopology_separateStreamsPerSubject_replyIsNotDeduped() throws Exception {
        String jobSubject = "jobs." + SAFE_TOPIC;
        String replySubject = "jobs." + SAFE_TOPIC + ".reply";

        JetStreamManagement jsm = connection.jetStreamManagement();
        jsm.addStream(StreamConfiguration.builder()
                .name("SAFE-JOBS-STREAM")
                .subjects(jobSubject)
                .retentionPolicy(RetentionPolicy.Limits)
                .storageType(StorageType.File)
                .duplicateWindow(Duration.ofSeconds(120))
                .build());
        jsm.addStream(StreamConfiguration.builder()
                .name("SAFE-REPLY-STREAM")
                .subjects(replySubject)
                .retentionPolicy(RetentionPolicy.Limits)
                .storageType(StorageType.File)
                .duplicateWindow(Duration.ofSeconds(120))
                .build());

        JetStream jetStream = connection.jetStream();
        String externalTaskId = "task-safe-1";

        publish(jetStream, jobSubject, externalTaskId, "job-payload");
        publish(jetStream, replySubject, externalTaskId, "reply-payload");

        connection.flush(Duration.ofSeconds(2));
        Thread.sleep(500);

        long jobStreamCount = jsm.getStreamInfo("SAFE-JOBS-STREAM").getStreamState().getMsgCount();
        long replyStreamCount = jsm.getStreamInfo("SAFE-REPLY-STREAM").getStreamState().getMsgCount();

        // This is the topology nats-bpm-bench's BenchEnvironment.ensureStreams() and the asyncapi
        // per-channel x-jetstream blocks already use — separate streams make the SAME Nats-Msg-Id
        // on job and reply subjects harmless (dedup windows never overlap across streams).
        assertThat(jobStreamCount).isEqualTo(1L);
        assertThat(replyStreamCount)
                .as("With separate streams per subject, the reply is NOT treated as a duplicate "
                        + "of its own job, even though both carry the same Nats-Msg-Id")
                .isEqualTo(1L);
    }

    private void publish(JetStream jetStream, String subject, String natsMsgId, String payload) throws Exception {
        Headers headers = new Headers();
        headers.add(NatsJetStreamConstants.MSG_ID_HDR, natsMsgId);
        NatsMessage msg = NatsMessage.builder()
                .subject(subject)
                .headers(headers)
                .data(payload.getBytes(StandardCharsets.UTF_8))
                .build();
        jetStream.publish(msg);
    }
}
