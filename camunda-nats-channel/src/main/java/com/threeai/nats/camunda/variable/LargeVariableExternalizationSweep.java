package com.threeai.nats.camunda.variable;

import static net.logstash.logback.argument.StructuredArguments.kv;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import javax.sql.DataSource;

import com.threeai.nats.core.jetstream.SweepLeaderLease;
import com.threeai.nats.core.largepayload.LargeVariableExternalizationProperties;
import com.threeai.nats.core.largepayload.LargeVariableSerializerNames;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

/**
 * Leader-elected, catch-all externalization sweep (`docs/08-large-variable-externalization.md`
 * D-A' "post-commit/downstream" — the SLOW path, mirroring {@code HistoryOutboxRelay}'s scheduled-
 * leader pattern). Picks up any variable {@link LargeVariableSerializer#writeValue}'s fast-path
 * post-commit listener missed (node crash between commit and dispatch, a WARN-logged externalization
 * failure, ...): the fast path is best-effort by design (D-A'/docs/08 §5), so this sweep is what
 * makes eventual offload a GUARANTEE rather than a hope.
 *
 * <p><b>CODER-NOTE (raw SQL against {@code ACT_RU_VARIABLE}/{@code ACT_GE_BYTEARRAY}, not the
 * command/query API):</b> there is no {@code VariableInstanceQuery} filter for "byte-array length"
 * or "not yet externalized" — finding CANDIDATES is a genuinely bulk, cross-cutting scan a
 * per-row command-API walk cannot do efficiently. This is READ-ONLY (candidate discovery only —
 * the actual mutation still goes through {@link LargeVariablePostCommitExternalizer#externalizeNow},
 * the engine's own {@code DbEntityManager}/OCC path) and depends only on the stable, well-known
 * camunda 7.x physical column names ({@code ID_}, {@code TYPE_}, {@code BYTEARRAY_ID_}, {@code
 * BYTES_}) — the SAME physical-schema stability D-E' already requires for "zero fork schema
 * change" to be possible at all. {@code TYPE_ IN (nats-ext-bytes, nats-ext-object, nats-ext-file)}
 * (never the built-in serializer names) is deliberate — see {@code LargeVariableSerializerNames}'
 * own CODER-NOTE: only rows THIS module's serializer wrote can ever be legitimate "staged, not yet
 * externalized" candidates.
 */
public class LargeVariableExternalizationSweep {

    private static final Logger log = LoggerFactory.getLogger(LargeVariableExternalizationSweep.class);
    private static final int BATCH_SIZE = 500;

    private static final String FIND_CANDIDATES_SQL =
            "SELECT v.ID_ FROM ACT_RU_VARIABLE v JOIN ACT_GE_BYTEARRAY b ON v.BYTEARRAY_ID_ = b.ID_ "
          + "WHERE v.TYPE_ IN (?, ?, ?) AND octet_length(b.BYTES_) > ? LIMIT " + BATCH_SIZE;

    private final DataSource engineDataSource;
    private final SweepLeaderLease leaderLease;
    private final LargeVariablePostCommitExternalizer externalizer;
    private final LargeVariableExternalizationProperties properties;
    private final String engineId;

    public LargeVariableExternalizationSweep(DataSource engineDataSource, SweepLeaderLease leaderLease,
            LargeVariablePostCommitExternalizer externalizer, LargeVariableExternalizationProperties properties,
            String engineId) {
        this.engineDataSource = engineDataSource;
        this.leaderLease = leaderLease;
        this.externalizer = externalizer;
        this.properties = properties;
        this.engineId = engineId;
    }

    @Scheduled(fixedDelayString = "${history.large-variable.sweep-cycle-period-seconds:60}000")
    public void sweepCycle() {
        if (!properties.isEnabled()) {
            return;
        }
        if (!leaderLease.tryAcquireOrRenew()) {
            return; // not the leader -- zero DB reads (basamak-1/2 parity, ADR-0002)
        }
        List<String> candidateVariableIds;
        try {
            candidateVariableIds = findCandidates();
        } catch (SQLException e) {
            // SYS_LARGE_VARIABLE_SWEEP_QUERY_FAILED -- cycle skipped, retried next cycle.
            log.error("Large-variable sweep candidate query failed — cycle skipped, retried next cycle",
                    kv("engine_id", engineId), e);
            return;
        }
        for (String variableId : candidateVariableIds) {
            externalizer.externalizeNow(variableId);
        }
    }

    private List<String> findCandidates() throws SQLException {
        List<String> ids = new ArrayList<>();
        try (Connection connection = engineDataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement(FIND_CANDIDATES_SQL)) {
            stmt.setString(1, LargeVariableSerializerNames.BYTES);
            stmt.setString(2, LargeVariableSerializerNames.OBJECT);
            stmt.setString(3, LargeVariableSerializerNames.FILE);
            stmt.setInt(4, properties.getThresholdBytes());
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    ids.add(rs.getString(1));
                }
            }
        }
        return ids;
    }
}
