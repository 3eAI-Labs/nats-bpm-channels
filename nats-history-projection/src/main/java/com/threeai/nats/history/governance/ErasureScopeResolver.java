package com.threeai.nats.history.governance;

import static net.logstash.logback.argument.StructuredArguments.kv;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import javax.sql.DataSource;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.threeai.nats.history.governance.ScopeResolution.CandidateInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * BA-Q6 kapsam-onayı akışı — {@code businessKey}/{@code userId} resolves to &gt;1 time-disjoint
 * instance groups → writes {@code erasure_scope_confirmation} and returns AMBIGUOUS
 * ({@code VAL_ERASURE_SUBJECT_KEY_AMBIGUOUS}) — pipeline does NOT auto-trigger. Single
 * unambiguous group → returns RESOLVED, pipeline proceeds directly.
 *
 * <p><b>Telco MSISDN churn protection:</b> a business key (e.g. an MSISDN) can be reassigned to a
 * DIFFERENT real subject over time — process instances sharing the same {@code business_key} but
 * separated by a large time gap ({@link #TIME_DISJOINT_GAP}, default 90 days) are treated as
 * potentially DIFFERENT subjects, requiring explicit confirmation before erasure proceeds.
 */
public class ErasureScopeResolver {

    private static final Logger log = LoggerFactory.getLogger(ErasureScopeResolver.class);
    private static final ObjectMapper JSON = new ObjectMapper().registerModule(new JavaTimeModule());
    private static final Duration TIME_DISJOINT_GAP = Duration.ofDays(90);

    private final DataSource projectionDataSource;

    public ErasureScopeResolver(DataSource projectionDataSource) {
        this.projectionDataSource = projectionDataSource;
    }

    public ScopeResolution resolve(String subjectKey) {
        List<CandidateInstance> allInstances = findInstancesByBusinessKey(subjectKey);
        List<List<CandidateInstance>> groups = clusterByTimeGap(allInstances);

        UUID requestId = UUID.randomUUID();
        if (groups.size() <= 1) {
            log.info("Erasure scope resolved unambiguously", kv("request_id", requestId), kv("instance_count", allInstances.size()));
            return new ScopeResolution.Resolved(requestId, allInstances);
        }

        writeScopeConfirmation(requestId, subjectKey, allInstances);
        log.warn("Erasure scope ambiguous — time-disjoint instance groups detected, confirmation required",
                kv("request_id", requestId), kv("group_count", groups.size())); // VAL_ERASURE_SUBJECT_KEY_AMBIGUOUS
        return new ScopeResolution.Ambiguous(requestId, allInstances);
    }

    /** Called once the requester confirms a subset of {@code erasure_scope_confirmation.candidate_instances}. */
    public void confirmScope(UUID requestId, List<String> confirmedInstanceIds, String confirmedBy) {
        String sql = "UPDATE erasure_scope_confirmation SET status = 'CONFIRMED', confirmed_scope = CAST(? AS JSONB), "
                + "confirmed_by = ?, confirmed_at = now() WHERE request_id = ?";
        try (Connection connection = projectionDataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, JSON.writeValueAsString(confirmedInstanceIds));
            stmt.setString(2, confirmedBy);
            stmt.setObject(3, requestId);
            stmt.executeUpdate();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to confirm erasure scope for request " + requestId, e);
        }
    }

    private List<CandidateInstance> findInstancesByBusinessKey(String subjectKey) {
        String sql = "SELECT process_instance_id, start_time, end_time FROM process_instance_history "
                + "WHERE business_key = ? AND deleted_at IS NULL ORDER BY start_time ASC";
        List<CandidateInstance> results = new ArrayList<>();
        try (Connection connection = projectionDataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, subjectKey);
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    Timestamp start = rs.getTimestamp("start_time");
                    Timestamp end = rs.getTimestamp("end_time");
                    results.add(new CandidateInstance(rs.getString("process_instance_id"),
                            start != null ? start.toInstant() : null, end != null ? end.toInstant() : null));
                }
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to resolve erasure candidates for subject key", e);
        }
        return results;
    }

    private List<List<CandidateInstance>> clusterByTimeGap(List<CandidateInstance> instances) {
        List<List<CandidateInstance>> groups = new ArrayList<>();
        List<CandidateInstance> currentGroup = new ArrayList<>();
        Instant previousStart = null;
        for (CandidateInstance instance : instances) {
            if (previousStart != null && instance.timeRangeStart() != null
                    && Duration.between(previousStart, instance.timeRangeStart()).compareTo(TIME_DISJOINT_GAP) > 0) {
                groups.add(currentGroup);
                currentGroup = new ArrayList<>();
            }
            currentGroup.add(instance);
            if (instance.timeRangeStart() != null) {
                previousStart = instance.timeRangeStart();
            }
        }
        if (!currentGroup.isEmpty()) {
            groups.add(currentGroup);
        }
        return groups;
    }

    private void writeScopeConfirmation(UUID requestId, String subjectKey, List<CandidateInstance> candidates) {
        String sql = "INSERT INTO erasure_scope_confirmation (request_id, subject_key, candidate_instances) "
                + "VALUES (?,?, CAST(? AS JSONB))";
        try (Connection connection = projectionDataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setObject(1, requestId);
            stmt.setString(2, subjectKey);
            stmt.setString(3, JSON.writeValueAsString(candidates));
            stmt.executeUpdate();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to write erasure_scope_confirmation for request " + requestId, e);
        }
    }
}
