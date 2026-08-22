package com.threeai.nats.flowable.externalworker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

import com.threeai.nats.core.headers.BpmHeaders;
import io.nats.client.impl.NatsMessage;
import io.nats.client.support.NatsJetStreamConstants;
import org.flowable.job.api.ExternalWorkerJob;
import org.junit.jupiter.api.Test;

/** D-I'v2: envelope identity + generation-scoped dedup key + nonce-only-on-the-wire. */
class EwJobMessageFactoryTest {

    private ExternalWorkerJob job(String id, String tenantId) {
        ExternalWorkerJob job = mock(ExternalWorkerJob.class);
        when(job.getId()).thenReturn(id);
        when(job.getProcessInstanceId()).thenReturn("pi-1");
        when(job.getTenantId()).thenReturn(tenantId);
        return job;
    }

    @Test
    void subject_headers_andDedupKey_areGenerationScoped() {
        NatsMessage msg = EwJobMessageFactory.build(job("job-9", null), "orders", "n0nce12345", "bk-7", Map.of());

        assertThat(msg.getSubject()).isEqualTo("ewjobs.orders");
        assertThat(msg.getHeaders().getFirst(NatsJetStreamConstants.MSG_ID_HDR)).isEqualTo("job-9#n0nce12345");
        assertThat(msg.getHeaders().getFirst(EwHeaders.LOCK_NONCE)).isEqualTo("n0nce12345");
        assertThat(msg.getHeaders().getFirst(BpmHeaders.CORRELATION_ID)).isEqualTo("job-9");
        // The sentinel half of the token must never travel on the wire (N-red-1).
        msg.getHeaders().forEach((k, v) -> assertThat(v).noneMatch(val -> val.contains("#n0nce12345#")));
    }

    @Test
    void payload_carriesIdentityFields_andOptionalOnesOnlyWhenPresent() {
        String json = new String(EwJobMessageFactory
                .build(job("job-9", "tenant-a"), "orders", "n0nce12345", null, Map.of())
                .getData(), StandardCharsets.UTF_8);

        assertThat(json).contains("\"jobId\":\"job-9\"")
                .contains("\"topic\":\"orders\"")
                .contains("\"processInstanceId\":\"pi-1\"")
                .contains("\"tenantId\":\"tenant-a\"")
                .doesNotContain("businessKey")
                .doesNotContain("variables");
    }

    @Test
    void variables_serializeTypedScalars() {
        Map<String, Object> vars = new LinkedHashMap<>();
        vars.put("count", 3);
        vars.put("ok", true);
        vars.put("note", "a\"b");
        String json = new String(EwJobMessageFactory
                .build(job("j", null), "t", "nonce1234", "bk", vars).getData(), StandardCharsets.UTF_8);

        assertThat(json).contains("\"variables\":{\"count\":3,\"ok\":true,\"note\":\"a\\\"b\"}")
                .contains("\"businessKey\":\"bk\"");
    }

    @Test
    void mintedNonce_alwaysSatisfiesWirePattern() {
        for (int i = 0; i < 100; i++) {
            assertThat(EwHeaders.NONCE_PATTERN.matcher(EwHeaders.mintNonce()).matches()).isTrue();
        }
    }
}
