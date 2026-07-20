package com.threeai.nats.history.query;

import java.time.Instant;
import java.util.UUID;

import org.slf4j.MDC;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Optional standalone REST exposure of {@link HistoryQueryApi} (ARCH-Q4). Embeddable-library
 * mode (tenant wires {@link HistoryQueryApi} directly into their own gateway) does not require
 * this controller to be active — see the module auto-configuration's conditional wiring.
 * Kontrat: {@code api/openapi.yaml} — this controller's paths/response shapes mirror it exactly,
 * no inline schema duplication (ADR-0014).
 *
 * <p><b>CODER-NOTE (QueryContext resolution):</b> the openapi contract declares {@code
 * bearerAuth} as a placeholder (SRS §4.7 pluggable authz) with no concrete claims schema — this
 * controller resolves a minimal {@link QueryContext} from the current request without assuming
 * any specific token format (a real deployment typically overrides this via a
 * {@code HandlerMethodArgumentResolver} or a servlet filter populating a request attribute; wiring
 * that is deployment-specific and out of this module's scope, ARCH-Q4).
 */
@RestController
@RequestMapping("/api/v1/history")
public class HistoryQueryController {

    private final HistoryQueryApi historyQueryApi;
    private final HistoryQueryAuthzSpi authzSpi;

    public HistoryQueryController(HistoryQueryApi historyQueryApi, HistoryQueryAuthzSpi authzSpi) {
        this.historyQueryApi = historyQueryApi;
        this.authzSpi = authzSpi;
    }

    @GetMapping("/process-instances/{processInstanceId}")
    public ResponseEntity<ApiResponse<ProcessInstanceSummary>> getProcessInstanceHistory(
            @PathVariable String processInstanceId) {
        String traceId = traceId();
        try {
            return historyQueryApi.getProcessInstanceHistory(processInstanceId, resolveContext())
                    .map(data -> ResponseEntity.ok(ApiResponse.ok(data)))
                    .orElseGet(() -> ResponseEntity.status(HttpStatus.NOT_FOUND)
                            .body(ApiResponse.error("RES_HISTORY_INSTANCE_NOT_FOUND",
                                    "Instance projeksiyonda bulunamadı.", traceId)));
        } catch (SecurityException denied) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN).body(ApiResponse.error(
                    "AUTH_QUERY_ACCESS_DENIED", "Bu kaynağa erişim yetkiniz yok.", traceId));
        }
    }

    @GetMapping("/process-instances")
    public ResponseEntity<ApiResponse<?>> listProcessInstanceHistory(
            @RequestParam(required = false) String businessKey,
            @RequestParam(required = false) String processDefinitionKey,
            @RequestParam(required = false) Instant startedAfter,
            @RequestParam(required = false) Instant startedBefore,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size) {
        String traceId = traceId();
        try {
            ProcessInstanceListQuery query = new ProcessInstanceListQuery(businessKey, processDefinitionKey,
                    startedAfter, startedBefore, new PageRequest(page, size));
            PagedResponse<ProcessInstanceSummary> result = historyQueryApi.listProcessInstanceHistory(query, resolveContext());
            return ResponseEntity.ok(ApiResponse.ok(result.data(), result.page(), result.size(), result.total(), traceId));
        } catch (IllegalArgumentException unsupportedPattern) {
            return ResponseEntity.badRequest().body(ApiResponse.error("VAL_QUERY_UNSUPPORTED_PATTERN",
                    "İstek desteklenen çekirdek-4 okuma desenine uymuyor (kapsam dışı).", traceId));
        } catch (SecurityException denied) {
            return accessDenied(traceId);
        }
    }

    @GetMapping("/process-instances/{processInstanceId}/activities")
    public ResponseEntity<ApiResponse<?>> listActivityHistory(@PathVariable String processInstanceId,
            @RequestParam(defaultValue = "0") int page, @RequestParam(defaultValue = "20") int size) {
        String traceId = traceId();
        try {
            return historyQueryApi.listActivityHistory(processInstanceId, new PageRequest(page, size), resolveContext())
                    .<ResponseEntity<ApiResponse<?>>>map(result -> ResponseEntity.ok(
                            ApiResponse.ok(result.data(), result.page(), result.size(), result.total(), traceId)))
                    .orElseGet(() -> notFound(traceId));
        } catch (SecurityException denied) {
            return accessDenied(traceId);
        }
    }

    @GetMapping("/process-instances/{processInstanceId}/tasks")
    public ResponseEntity<ApiResponse<?>> listTaskHistory(@PathVariable String processInstanceId,
            @RequestParam(defaultValue = "0") int page, @RequestParam(defaultValue = "20") int size) {
        String traceId = traceId();
        try {
            return historyQueryApi.listTaskHistory(processInstanceId, new PageRequest(page, size), resolveContext())
                    .<ResponseEntity<ApiResponse<?>>>map(result -> ResponseEntity.ok(
                            ApiResponse.ok(result.data(), result.page(), result.size(), result.total(), traceId)))
                    .orElseGet(() -> notFound(traceId));
        } catch (SecurityException denied) {
            return accessDenied(traceId);
        }
    }

    @GetMapping("/process-instances/{processInstanceId}/variables")
    public ResponseEntity<ApiResponse<?>> listVariableHistory(@PathVariable String processInstanceId,
            @RequestParam(defaultValue = "0") int page, @RequestParam(defaultValue = "20") int size) {
        String traceId = traceId();
        try {
            return historyQueryApi.listVariableHistory(processInstanceId, new PageRequest(page, size), resolveContext())
                    .<ResponseEntity<ApiResponse<?>>>map(result -> ResponseEntity.ok(
                            ApiResponse.ok(result.data(), result.page(), result.size(), result.total(), traceId)))
                    .orElseGet(() -> notFound(traceId));
        } catch (SecurityException denied) {
            return accessDenied(traceId);
        }
    }

    private QueryContext resolveContext() {
        String callerIdentity = MDC.get("trace_id"); // placeholder identity, see class Javadoc CODER-NOTE
        QueryContext base = new QueryContext(callerIdentity, java.util.Set.of(), false);
        return base.withPiiViewPermitted(authzSpi.hasPiiViewPermission(base));
    }

    private static String traceId() {
        String existing = MDC.get("trace_id");
        return existing != null ? existing : UUID.randomUUID().toString();
    }

    private static ResponseEntity<ApiResponse<?>> notFound(String traceId) {
        ApiResponse<?> body = ApiResponse.error("RES_HISTORY_INSTANCE_NOT_FOUND", "Instance projeksiyonda bulunamadı.", traceId);
        return ResponseEntity.status(HttpStatus.NOT_FOUND).body(body);
    }

    private static ResponseEntity<ApiResponse<?>> accessDenied(String traceId) {
        ApiResponse<?> body = ApiResponse.error("AUTH_QUERY_ACCESS_DENIED", "Bu kaynağa erişim yetkiniz yok.", traceId);
        return ResponseEntity.status(HttpStatus.FORBIDDEN).body(body);
    }
}
