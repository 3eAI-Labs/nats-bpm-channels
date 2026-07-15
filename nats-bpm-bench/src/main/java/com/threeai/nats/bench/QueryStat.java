package com.threeai.nats.bench;

/** One {@code pg_stat_statements} row: a query fingerprint (queryid), its normalized text, and call count. */
public record QueryStat(long queryId, String query, long calls) {
}
