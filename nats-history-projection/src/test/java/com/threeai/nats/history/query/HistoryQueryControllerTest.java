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
}
