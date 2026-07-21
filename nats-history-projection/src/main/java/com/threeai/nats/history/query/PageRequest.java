package com.threeai.nats.history.query;

/** {@code openapi.yaml} {@code Page}/{@code Size} query parameters. */
public record PageRequest(int page, int size) {

    public PageRequest {
        if (page < 0) {
            page = 0;
        }
        if (size < 1) {
            size = 20;
        }
        if (size > 200) {
            size = 200;
        }
    }

    public static PageRequest of(int page, int size) {
        return new PageRequest(page, size);
    }

    public int offset() {
        return page * size;
    }
}
