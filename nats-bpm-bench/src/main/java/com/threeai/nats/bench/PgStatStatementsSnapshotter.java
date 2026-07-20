package com.threeai.nats.bench;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import javax.sql.DataSource;

/**
 * Reads {@code pg_stat_statements} for queries touching a given table-name pattern — the bench
 * module's own DB-access role is read-only introspection; it never issues DML against the
 * engine schema (03_classes/5_bench.md §1, US-D1).
 */
public class PgStatStatementsSnapshotter {

    private static final String EXT_TASK_LIKE_PATTERN = "%ACT_RU_EXT_TASK%";
    private static final String SELECT_STATS_SQL =
            "SELECT queryid, query, calls FROM pg_stat_statements WHERE query ILIKE ? ORDER BY calls DESC";

    public void reset(DataSource dataSource) throws SQLException {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement("SELECT pg_stat_statements_reset()")) {
            stmt.execute();
        }
    }

    /** Basamak-1 default: {@code ACT_RU_EXT_TASK}-touching queries only. */
    public Snapshot capture(DataSource dataSource) throws SQLException {
        return capture(dataSource, EXT_TASK_LIKE_PATTERN);
    }

    /**
     * @param likePattern an {@code ILIKE} pattern (e.g. {@code "%ACT_HI_%"}) — generalized
     *                    beyond the basamak-1 {@code ACT_RU_EXT_TASK}-only default so basamak-2's
     *                    {@code HistoryBenchScenario} can reuse this class rather than
     *                    hand-rolling its own {@code pg_stat_statements} query.
     */
    public Snapshot capture(DataSource dataSource, String likePattern) throws SQLException {
        List<QueryStat> stats = new ArrayList<>();
        try (Connection connection = dataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement(SELECT_STATS_SQL)) {
            stmt.setString(1, likePattern);
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    stats.add(new QueryStat(rs.getLong("queryid"), rs.getString("query"), rs.getLong("calls")));
                }
            }
        }
        return new Snapshot(stats);
    }
}
