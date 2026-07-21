package com.threeai.nats.history.query;

import java.util.List;

/** {@code openapi.yaml} {@code ApiResponse} envelope + {@code PageMeta}, generic over the item type. */
public record PagedResponse<T>(List<T> data, int page, int size, long total) {
}
