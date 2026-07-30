package com.threeai.nats.history.query;

/**
 * Standard response envelope (`openapi.yaml` {@code ApiResponse}/{@code PageMeta}) — follows the
 * project's API-design and error-handling conventions.
 */
public record ApiResponse<T>(boolean success, String message, String code, T data, PageMeta meta) {

    public record PageMeta(Integer page, Integer size, Long total, String traceId) {
    }

    public static <T> ApiResponse<T> ok(T data) {
        return new ApiResponse<>(true, null, null, data, null);
    }

    public static <T> ApiResponse<T> ok(T data, int page, int size, long total, String traceId) {
        return new ApiResponse<>(true, null, null, data, new PageMeta(page, size, total, traceId));
    }

    public static <T> ApiResponse<T> error(String code, String message, String traceId) {
        return new ApiResponse<>(false, message, code, null, new PageMeta(null, null, null, traceId));
    }
}
