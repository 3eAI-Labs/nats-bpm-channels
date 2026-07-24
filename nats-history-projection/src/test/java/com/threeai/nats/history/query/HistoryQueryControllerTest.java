package com.threeai.nats.history.query;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

class HistoryQueryControllerTest {

    private HistoryQueryApi historyQueryApi;
    private HistoryQueryAuthzSpi authzSpi;
    private HistoryQueryController controller;

    @BeforeEach
    void setUp() {
        historyQueryApi = mock(HistoryQueryApi.class);
        authzSpi = mock(HistoryQueryAuthzSpi.class);
        when(authzSpi.hasPiiViewPermission(any())).thenReturn(true);
        controller = new HistoryQueryController(historyQueryApi, authzSpi);
    }

    @Test
    void getProcessInstanceHistory_found_returns200() {
        ProcessInstanceSummary summary = new ProcessInstanceSummary("p1", "def", "biz", "user",
                Instant.now(), null, "ACTIVE", "DUAL_RUN");
        when(historyQueryApi.getProcessInstanceHistory(anyString(), any())).thenReturn(Optional.of(summary));

        ResponseEntity<ApiResponse<ProcessInstanceSummary>> response = controller.getProcessInstanceHistory("p1");

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody().success()).isTrue();
        assertThat(response.getBody().data().processInstanceId()).isEqualTo("p1");
    }

    @Test
    void getProcessInstanceHistory_notFound_returns404WithErrorCode() {
        when(historyQueryApi.getProcessInstanceHistory(anyString(), any())).thenReturn(Optional.empty());

        ResponseEntity<ApiResponse<ProcessInstanceSummary>> response = controller.getProcessInstanceHistory("missing");

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(response.getBody().code()).isEqualTo("RES_HISTORY_INSTANCE_NOT_FOUND");
    }

    @Test
    void getProcessInstanceHistory_accessDenied_returns403() {
        when(historyQueryApi.getProcessInstanceHistory(anyString(), any())).thenThrow(new SecurityException("denied"));

        ResponseEntity<ApiResponse<ProcessInstanceSummary>> response = controller.getProcessInstanceHistory("p1");

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.FORBIDDEN);
        assertThat(response.getBody().code()).isEqualTo("AUTH_QUERY_ACCESS_DENIED");
    }

    @Test
    void listProcessInstanceHistory_unsupportedPattern_returns400() {
        when(historyQueryApi.listProcessInstanceHistory(any(), any()))
                .thenThrow(new IllegalArgumentException("no filter"));

        ResponseEntity<ApiResponse<?>> response = controller.listProcessInstanceHistory(
                null, null, null, null, 0, 20);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(response.getBody().code()).isEqualTo("VAL_QUERY_UNSUPPORTED_PATTERN");
    }

    @Test
    void listActivityHistory_instanceNotFound_returns404() {
        when(historyQueryApi.listActivityHistory(anyString(), any(), any())).thenReturn(Optional.empty());

        ResponseEntity<ApiResponse<?>> response = controller.listActivityHistory("missing", 0, 20);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }

    // --- Sentinel Phase 5.5 (round 2) coverage: success + accessDenied on every remaining endpoint ---

    @Test
    void listProcessInstanceHistory_found_returns200WithPageMeta() {
        ProcessInstanceSummary summary = new ProcessInstanceSummary("p1", "def", "biz", "user",
                Instant.now(), null, "ACTIVE", "DUAL_RUN");
        PagedResponse<ProcessInstanceSummary> paged = new PagedResponse<>(java.util.List.of(summary), 0, 20, 1L);
        when(historyQueryApi.listProcessInstanceHistory(any(), any())).thenReturn(paged);

        ResponseEntity<ApiResponse<?>> response = controller.listProcessInstanceHistory(
                "biz", null, null, null, 0, 20);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody().success()).isTrue();
        assertThat(response.getBody().meta().total()).isEqualTo(1L);
    }

    @Test
    void listProcessInstanceHistory_accessDenied_returns403() {
        when(historyQueryApi.listProcessInstanceHistory(any(), any())).thenThrow(new SecurityException("denied"));

        ResponseEntity<ApiResponse<?>> response = controller.listProcessInstanceHistory(
                "biz", null, null, null, 0, 20);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.FORBIDDEN);
        assertThat(response.getBody().code()).isEqualTo("AUTH_QUERY_ACCESS_DENIED");
    }

    @Test
    void listActivityHistory_found_returns200WithPageMeta() {
        ActivityHistoryEntry entry = new ActivityHistoryEntry("act1", "userTask", "u1", Instant.now(), null);
        PagedResponse<ActivityHistoryEntry> paged = new PagedResponse<>(java.util.List.of(entry), 0, 20, 1L);
        when(historyQueryApi.listActivityHistory(anyString(), any(), any())).thenReturn(Optional.of(paged));

        ResponseEntity<ApiResponse<?>> response = controller.listActivityHistory("p1", 0, 20);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody().success()).isTrue();
    }

    @Test
    void listActivityHistory_accessDenied_returns403() {
        when(historyQueryApi.listActivityHistory(anyString(), any(), any())).thenThrow(new SecurityException("denied"));

        ResponseEntity<ApiResponse<?>> response = controller.listActivityHistory("p1", 0, 20);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.FORBIDDEN);
        assertThat(response.getBody().code()).isEqualTo("AUTH_QUERY_ACCESS_DENIED");
    }

    @Test
    void listTaskHistory_found_returns200WithPageMeta() {
        TaskHistoryEntry entry = new TaskHistoryEntry("t1", "Approve", "u1", "u2", Instant.now(), null);
        PagedResponse<TaskHistoryEntry> paged = new PagedResponse<>(java.util.List.of(entry), 0, 20, 1L);
        when(historyQueryApi.listTaskHistory(anyString(), any(), any())).thenReturn(Optional.of(paged));

        ResponseEntity<ApiResponse<?>> response = controller.listTaskHistory("p1", 0, 20);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody().success()).isTrue();
    }

    @Test
    void listTaskHistory_instanceNotFound_returns404() {
        when(historyQueryApi.listTaskHistory(anyString(), any(), any())).thenReturn(Optional.empty());

        ResponseEntity<ApiResponse<?>> response = controller.listTaskHistory("missing", 0, 20);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }

    @Test
    void listTaskHistory_accessDenied_returns403() {
        when(historyQueryApi.listTaskHistory(anyString(), any(), any())).thenThrow(new SecurityException("denied"));

        ResponseEntity<ApiResponse<?>> response = controller.listTaskHistory("p1", 0, 20);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.FORBIDDEN);
        assertThat(response.getBody().code()).isEqualTo("AUTH_QUERY_ACCESS_DENIED");
    }

    @Test
    void listVariableHistory_found_returns200WithPageMeta() {
        VariableHistoryEntry entry = new VariableHistoryEntry("orderId", "String", "123", false, Instant.now());
        PagedResponse<VariableHistoryEntry> paged = new PagedResponse<>(java.util.List.of(entry), 0, 20, 1L);
        when(historyQueryApi.listVariableHistory(anyString(), any(), any())).thenReturn(Optional.of(paged));

        ResponseEntity<ApiResponse<?>> response = controller.listVariableHistory("p1", 0, 20);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody().success()).isTrue();
    }

    @Test
    void listVariableHistory_instanceNotFound_returns404() {
        when(historyQueryApi.listVariableHistory(anyString(), any(), any())).thenReturn(Optional.empty());

        ResponseEntity<ApiResponse<?>> response = controller.listVariableHistory("missing", 0, 20);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }

    @Test
    void listVariableHistory_accessDenied_returns403() {
        when(historyQueryApi.listVariableHistory(anyString(), any(), any())).thenThrow(new SecurityException("denied"));

        ResponseEntity<ApiResponse<?>> response = controller.listVariableHistory("p1", 0, 20);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.FORBIDDEN);
        assertThat(response.getBody().code()).isEqualTo("AUTH_QUERY_ACCESS_DENIED");
    }
}
