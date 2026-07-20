package com.threeai.nats.history.cutover;

import static net.logstash.logback.argument.StructuredArguments.kv;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import javax.sql.DataSource;

import com.threeai.nats.core.history.HistoryClassNames;
import com.threeai.nats.core.metrics.NatsChannelMetrics;
import com.threeai.nats.history.cutover.ClassCutoverState.ConsistencyPath;
import com.threeai.nats.history.cutover.ClassCutoverState.CutoverState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

/**
 * Scheduled diff-detection + clean-streak gate (BR-CUT-001/004, ADR-0015,
 * `03_classes/4_cutover_reconciliation.md` §1). Per class: counts projection rows vs ACT_HI rows
 * (read-only JDBC against the engine DB, DP-14 — counters/ids only, never PII values). "Clean"
 * (BA-Q2): audit-critical → {@code diff==0}; bulk → {@code diff<=epsilon} (default 0, per-class
 * override) AND no increasing trend (compares against {@code last_diff_count}).
 *
 * <p><b>CODER-NOTE (RES_RECONCILIATION_DIFF_THRESHOLD_EXCEEDED interpretation):</b> the
 * {@code nats.history.reconciliation.diff_count} gauge + the Prometheus alert rule
 * (`10_metrics.md §2.5`, {@code increase(...[1h]) > 0}) already do the "sustained over time"
 * judgement externally. This class raises the escalated {@code
 * RES_RECONCILIATION_DIFF_THRESHOLD_EXCEEDED} log (vs. the routine {@code
 * BUS_RECONCILIATION_DIFF_DETECTED} WARN) specifically when a class that had ALREADY reached
 * {@code N_GUN_TEMIZ} or {@code CUTOVER_TALEP} regresses — a clean-readiness REGRESSION is a
 * stronger ops signal than an ordinary in-progress diff.
 */
public class ReconciliationJob {

    private static final Logger log = LoggerFactory.getLogger(ReconciliationJob.class);

    private final DataSource projectionDataSource;
    private final DataSource engineDataSourceReadOnly;
    private final ClassCutoverStateStore stateStore;
    private final NatsChannelMetrics metrics;
    private final ReconciliationProperties properties;
    private final String engineId;

    private final java.util.Map<String, AtomicLong> diffGaugeValues = new ConcurrentHashMap<>();
    private final java.util.Map<String, AtomicLong> streakGaugeValues = new ConcurrentHashMap<>();

    public ReconciliationJob(DataSource projectionDataSource, DataSource engineDataSourceReadOnly,
            ClassCutoverStateStore stateStore, NatsChannelMetrics metrics, ReconciliationProperties properties,
            String engineId) {
        this.projectionDataSource = projectionDataSource;
        this.engineDataSourceReadOnly = engineDataSourceReadOnly;
        this.stateStore = stateStore;
        this.metrics = metrics;
        this.properties = properties;
        this.engineId = engineId;
        if (metrics != null) {
            for (String historyClass : HistoryClassNames.ALL_CLASSES) {
                AtomicLong diffHolder = diffGaugeValues.computeIfAbsent(historyClass, c -> new AtomicLong(0));
                AtomicLong streakHolder = streakGaugeValues.computeIfAbsent(historyClass, c -> new AtomicLong(0));
                metrics.registerHistoryReconciliationDiffCountGauge(historyClass, diffHolder::get);
                metrics.registerHistoryReconciliationCleanStreakGauge(historyClass, streakHolder::get);
            }
        }
    }

    @Scheduled(cron = "${history.reconciliation.cron:0 0 3 * * *}")
    public void reconcileAllClasses() {
        for (String historyClass : HistoryClassNames.ALL_CLASSES) {
            try {
                reconcileClass(historyClass);
            } catch (Exception e) {
                // SYS_RECONCILIATION_JOB_FAILED -- log-only, streak untouched, cycle skipped.
                log.error("Reconciliation cycle failed for class — streak untouched, retried next cycle",
                        kv("history_class", historyClass), kv("engine_id", engineId), e);
            }
        }
    }

    private void reconcileClass(String historyClass) throws SQLException {
        ConsistencyPath path = isAuditCritical(historyClass) ? ConsistencyPath.AUDIT_CRITICAL : ConsistencyPath.BULK;
        ClassCutoverState state = stateStore.findOrCreate(engineId, historyClass, path, properties.cleanStreakTargetFor(historyClass));
        if (state.state() == CutoverState.CUTOVERLANMIS) {
            return; // already cut over -- nothing left to reconcile
        }

        long projectionCount = countProjection(historyClass);
        long engineCount = countEngine(historyClass);
        long diff = Math.abs(projectionCount - engineCount);
        updateGauges(historyClass, diff, state.cleanStreakDays());

        evaluateGate(state, diff);
    }

    /** Package-visible for direct unit testing without a full reconcileClass() DB round-trip. */
    protected void evaluateGate(ClassCutoverState state, long diffCount) {
        boolean clean = isClean(state, diffCount);
        if (!clean) {
            boolean wasNearCutover = state.state() == CutoverState.N_GUN_TEMIZ || state.state() == CutoverState.CUTOVER_TALEP;
            if (wasNearCutover) {
                log.error("Reconciliation diff detected on a class that had already reached cutover-readiness — regression",
                        kv("history_class", state.historyClass()), kv("engine_id", state.engineId()),
                        kv("diff_count", diffCount)); // RES_RECONCILIATION_DIFF_THRESHOLD_EXCEEDED
            } else {
                log.warn("Reconciliation diff detected — clean streak reset",
                        kv("history_class", state.historyClass()), kv("diff_count", diffCount)); // BUS_RECONCILIATION_DIFF_DETECTED
            }
            stateStore.resetStreak(state.engineId(), state.historyClass(), diffCount);
            return;
        }

        int newStreak = state.cleanStreakDays() + 1;
        CutoverState resultingState = newStreak >= state.cleanStreakTarget() ? CutoverState.N_GUN_TEMIZ : CutoverState.RECONCILING;
        stateStore.recordCleanCycle(state.engineId(), state.historyClass(), newStreak, diffCount, resultingState);
    }

    private boolean isClean(ClassCutoverState state, long diffCount) {
        if (state.consistencyPath() == ConsistencyPath.AUDIT_CRITICAL) {
            return diffCount == 0;
        }
        long epsilon = properties.bulkEpsilonFor(state.historyClass());
        boolean withinEpsilon = diffCount <= epsilon;
        boolean notIncreasing = diffCount <= state.lastDiffCount() || state.lastDiffCount() == 0;
        return withinEpsilon && notIncreasing;
    }

    private boolean isAuditCritical(String historyClass) {
        return HistoryClassNames.DEFAULT_AUDIT_CRITICAL_CLASSES.contains(historyClass);
    }

    private void updateGauges(String historyClass, long diff, int streakDays) {
        AtomicLong diffHolder = diffGaugeValues.get(historyClass);
        if (diffHolder != null) {
            diffHolder.set(diff);
        }
        AtomicLong streakHolder = streakGaugeValues.get(historyClass);
        if (streakHolder != null) {
            streakHolder.set(streakDays);
        }
    }

    private long countProjection(String historyClass) throws SQLException {
        String table = com.threeai.nats.history.projection.ProjectionStore.tableNameFor(historyClass);
        String sql = "SELECT count(*) FROM " + table + " WHERE engine_id = ?";
        try (Connection connection = projectionDataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, engineId);
            try (ResultSet rs = stmt.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        }
    }

    private long countEngine(String historyClass) throws SQLException {
        String table = ActHiTableNames.of(historyClass);
        String sql = "SELECT count(*) FROM " + table;
        try (Connection connection = engineDataSourceReadOnly.getConnection();
             PreparedStatement stmt = connection.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {
            rs.next();
            return rs.getLong(1);
        }
    }
}
