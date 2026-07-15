package com.threeai.nats.bench;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import javax.sql.DataSource;

/**
 * Reads {@code pg_stat_statements} for {@code ACT_RU_EXT_TASK}-touching queries — the bench
 * module's own DB-access role is read-only introspection; it never issues DML against the
 * engine schema (03_classes/5_bench.md §1, US-D1).
 */
public class PgStatStatementsSnapshotter {

    private static final String SELECT_STATS_SQL =
            "SELECT queryid, query, calls FROM pg_stat_statements WHERE query ILIKE '%ACT_RU_EXT_TASK%' "
                    + "ORDER BY calls DESC";

    public void reset(DataSource dataSource) throws SQLException {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement("SELECT pg_stat_statements_reset()")) {
            stmt.execute();
        }
    }

    public Snapshot capture(DataSource dataSource) throws SQLException {
        List<QueryStat> stats = new ArrayList<>();
        try (Connection connection = dataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement(SELECT_STATS_SQL);
             ResultSet rs = stmt.executeQuery()) {
            while (rs.next()) {
                stats.add(new QueryStat(rs.getLong("queryid"), rs.getString("query"), rs.getLong("calls")));
            }
        }
        return new Snapshot(stats);
    }
}
