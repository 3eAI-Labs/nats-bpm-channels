package com.threeai.nats.history.query;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.Set;

import org.junit.jupiter.api.Test;

class PiiMaskingServiceTest {

    private final PiiMaskingService maskingService = new PiiMaskingService();

    private QueryContext ctx(boolean piiPermitted) {
        return new QueryContext("user-1", Set.of(), piiPermitted);
    }

    @Test
    void mask_piiPermitted_returnsUnmodified() {
        ProcessInstanceSummary summary = new ProcessInstanceSummary("p1", "def1", "biz1", "real-user",
                Instant.now(), null, "ACTIVE", "DUAL_RUN");

        ProcessInstanceSummary result = maskingService.mask(summary, ctx(true));

        assertThat(result.startUserId()).isEqualTo("real-user");
    }

    @Test
    void mask_piiNotPermitted_masksStartUserId() {
        ProcessInstanceSummary summary = new ProcessInstanceSummary("p1", "def1", "biz1", "real-user",
                Instant.now(), null, "ACTIVE", "DUAL_RUN");

        ProcessInstanceSummary result = maskingService.mask(summary, ctx(false));

        assertThat(result.startUserId()).isEqualTo("***");
        assertThat(result.processInstanceId()).isEqualTo("p1"); // non-PII fields untouched
        assertThat(result.businessKey()).isEqualTo("biz1");
    }

    @Test
    void mask_activityHistoryEntry_masksAssignee() {
        ActivityHistoryEntry entry = new ActivityHistoryEntry("act1", "userTask", "real-assignee", Instant.now(), null);

        ActivityHistoryEntry result = maskingService.mask(entry, ctx(false));

        assertThat(result.assignee()).isEqualTo("***");
        assertThat(result.activityId()).isEqualTo("act1");
    }

    @Test
    void mask_taskHistoryEntry_masksAssigneeAndOwner() {
        TaskHistoryEntry entry = new TaskHistoryEntry("task1", "Review", "real-assignee", "real-owner", Instant.now(), null);

        TaskHistoryEntry result = maskingService.mask(entry, ctx(false));

        assertThat(result.assignee()).isEqualTo("***");
        assertThat(result.owner()).isEqualTo("***");
        assertThat(result.name()).isEqualTo("Review");
    }

    @Test
    void mask_variableHistoryEntry_masksValueAndSetsMaskedFlag() {
        VariableHistoryEntry entry = new VariableHistoryEntry("myVar", "String", "sensitive-value", false, Instant.now());

        VariableHistoryEntry result = maskingService.mask(entry, ctx(false));

        assertThat(result.value()).isEqualTo("***");
        assertThat(result.masked()).isTrue();
        assertThat(result.variableName()).isEqualTo("myVar");
    }

    @Test
    void mask_variableHistoryEntry_piiPermitted_notMasked() {
        VariableHistoryEntry entry = new VariableHistoryEntry("myVar", "String", "sensitive-value", false, Instant.now());

        VariableHistoryEntry result = maskingService.mask(entry, ctx(true));

        assertThat(result.value()).isEqualTo("sensitive-value");
        assertThat(result.masked()).isFalse();
    }

    @Test
    void mask_nullDto_returnsNull() {
        ProcessInstanceSummary result = maskingService.mask((ProcessInstanceSummary) null, ctx(false));

        assertThat(result).isNull();
    }
}
