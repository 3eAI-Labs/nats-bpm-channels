package com.threeai.nats.cadenzaflow.a2;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

import com.threeai.nats.core.headers.BpmHeaders;
import io.nats.client.impl.Headers;
import io.nats.client.impl.NatsMessage;
import io.nats.client.support.NatsJetStreamConstants;
import org.cadenzaflow.bpm.engine.impl.persistence.entity.ExternalTaskEntity;

/**
 * Shared {@code jobs.<topic>} message construction — used by both {@link A2PostCommitPublisher}
 * (fast-path dispatch) and {@link A2OrphanSweep} (cold re-publish) so the wire format never
 * drifts between the two producers.
 *
 * <p><b>CODER-NOTE:</b> process-variable/payload serialization is explicitly tenant-defined and
 * out of this repo's contract (asyncapi {@code OpaqueBusinessPayload}) — see CODER-QUESTIONS.
 * traceId/idempotencyKey are not first-class {@code ExternalTaskEntity} fields; reading them
 * from process variables here would require a DB round-trip BR-A2-004 forbids for the
 * post-commit path, so a fresh trace id is minted per dispatch/re-dispatch and
 * {@code externalTaskId} (already the natural, stable identity) is reused as the idempotency key.
 */
final class A2JobMessageFactory {

    private A2JobMessageFactory() {
    }

    static NatsMessage build(ExternalTaskEntity task) {
        String subject = "jobs." + task.getTopicName();
        return NatsMessage.builder()
                .subject(subject)
                .data(serialize(task))
                .headers(buildHeaders(task))
                .build();
    }

    private static Headers buildHeaders(ExternalTaskEntity task) {
        Headers h = BpmHeaders.build(UUID.randomUUID().toString(), task.getBusinessKey(), task.getId());
        h.add(NatsJetStreamConstants.MSG_ID_HDR, task.getId()); // A2 dedup key = externalTaskId (IR-3)
        return h;
    }

    private static byte[] serialize(ExternalTaskEntity task) {
        String businessKey = task.getBusinessKey() != null ? task.getBusinessKey() : "";
        String json = "{\"externalTaskId\":\"" + task.getId() + "\","
                + "\"topic\":\"" + task.getTopicName() + "\","
                + "\"businessKey\":\"" + businessKey + "\"}";
        return json.getBytes(StandardCharsets.UTF_8);
    }
}
