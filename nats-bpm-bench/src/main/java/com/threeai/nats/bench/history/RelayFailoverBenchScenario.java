package com.threeai.nats.bench.history;

import static net.logstash.logback.argument.StructuredArguments.kv;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import javax.sql.DataSource;

import com.threeai.nats.bench.BenchEnvironment;
import com.threeai.nats.camunda.history.CompactHistoryOutboxWriter;
import com.threeai.nats.camunda.history.HistoryClassificationProperties;
import com.threeai.nats.camunda.history.HistoryOutboxProperties;
import com.threeai.nats.camunda.history.HistoryOutboxRelay;
import com.threeai.nats.core.history.HistoryClassNames;
import com.threeai.nats.core.history.PseudonymTokenGenerator;
import com.threeai.nats.core.jetstream.JetStreamKvManager;
import com.threeai.nats.core.jetstream.SweepLeaderLease;
import org.camunda.bpm.engine.impl.history.event.UserOperationLogEntryEventEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code history-relay-leader} failover RTO/RPO measurement (`03_classes/5_bench.md` §3,
 * `01_overview.md` "Phase3'ün devrettiği doğrulamalar #5", `TEST_SPECIFICATIONS.md` (h)).
 *
 * <p><b>Real measurement (Phase 5.5, per explicit task instruction) — supersedes the Phase 5
 * design-only placeholder.</b> {@code engineNodeReplicaCount} in-process {@link
 * SweepLeaderLease}/{@link HistoryOutboxRelay} pairs are driven against the SAME real NATS
 * JetStream KV bucket (production TTL, {@code 2 * relayCyclePeriodSeconds} = 60s default) and the
 * SAME real {@code compact_history_outbox} table on {@link BenchEnvironment}'s Postgres
 * container — one relay object per simulated engine-node replica (matching the LLD procedure's
 * "her biri kendi {@code HistoryOutboxRelay} instance'ı"; a full Testcontainers/engine boot per
 * replica is not required — the failover contract lives entirely in the KV lease + outbox table,
 * not the BPMN engine).
 *
 * <h2>Procedure</h2>
 * <ol>
 *   <li>Replica 0 becomes leader (genuine {@code kv.create()} against the real bucket).</li>
 *   <li>Audit-critical outbox rows are seeded AFTER replica 0 holds the lease but BEFORE it ever
 *       drains them — "active relay work in flight" at the moment of the simulated crash.</li>
 *   <li>Replica 0 is "killed": this class simply stops invoking its {@code relayCycle()} /
 *       {@code tryAcquireOrRenew()} ever again (no graceful release) — its last-written KV
 *       revision ages out naturally via the bucket's TTL, exactly like a hard node crash
 *       (`docker kill`, not a graceful {@code close()}).</li>
 *   <li>The remaining replicas are polled (fast poll, see {@link #POLL_INTERVAL} — deliberately
 *       much shorter than the 30s production {@code relayCyclePeriodSeconds} so the measurement
 *       captures the TTL-expiry-driven recovery bound itself, not incidental standby-scheduling
 *       jitter) until exactly one acquires the lease.</li>
 *   <li>The new leader drains the seeded rows; final row count proves RPO (0 = no audit loss).</li>
 * </ol>
 *
 * <p><b>QA-FINDING (measured, not a defect — Phase 5.5):</b> because recovery is driven purely by
 * NATS KV TTL expiry of the crashed leader's last-written revision, a standby can, BY
 * CONSTRUCTION, never observe the key as free before {@code leaseTtl} has fully elapsed since that
 * last write. The empirically measured {@code timeToRecover} therefore sits at {@code leaseTtl}
 * PLUS this harness's own detection latency (bounded by {@link #POLL_INTERVAL}) — never
 * meaningfully under it. {@code recoveredWithinLeaseTtl} in {@link RelayFailoverReport} therefore
 * compares against {@code leaseTtl + POLL_INTERVAL} (the tightest bound this measurement technique
 * can honestly resolve), not a strict {@code <= leaseTtl}, which no TTL-expiry-based mechanism
 * could ever satisfy. Operationally: "RTO &lt;= 60s" should be read as "recovery completes at
 * approximately 60s, not sooner" — not as a bound with headroom.
 */
public class RelayFailoverBenchScenario {

    private static final Logger log = LoggerFactory.getLogger(RelayFailoverBenchScenario.class);
    private static final String ENGINE_ID = "camunda";
    private static final int SEEDED_ROW_COUNT = 5;
    private static final Duration POLL_INTERVAL = Duration.ofMillis(500);
    private static final Duration SAFETY_MARGIN = Duration.ofSeconds(30);
    private static final int DRAIN_ATTEMPTS = 5;

    public RelayFailoverReport run(BenchEnvironment env, int engineNodeReplicaCount) {
        if (engineNodeReplicaCount < 2) {
            throw new IllegalArgumentException(
                    "RelayFailoverBenchScenario needs >=2 engine-node replicas to observe a failover, got "
                            + engineNodeReplicaCount);
        }

        HistoryOutboxProperties properties = new HistoryOutboxProperties(); // production default: 30s cycle
        Duration ttl = Duration.ofSeconds(2 * properties.getRelayCyclePeriodSeconds()); // ADR-0002/LLD "#3" -- 60s

        String bucket = "history-relay-leader-failover-" + UUID.randomUUID();
        JetStreamKvManager kvManager = new JetStreamKvManager();
        kvManager.ensureBucket(bucket, ttl, 1, env.natsConnection());

        List<SweepLeaderLease> leases = new ArrayList<>(engineNodeReplicaCount);
        List<HistoryOutboxRelay> relays = new ArrayList<>(engineNodeReplicaCount);
        for (int i = 0; i < engineNodeReplicaCount; i++) {
            SweepLeaderLease lease = new SweepLeaderLease(env.jetStream(), kvManager, env.natsConnection(),
                    bucket, "relay-leader.", ENGINE_ID, "node-" + i, ttl);
            leases.add(lease);
            relays.add(new HistoryOutboxRelay(env.dataSource(), env.jetStream(), lease, properties, null, ENGINE_ID));
        }

        // 1. Replica 0 becomes leader -- genuine KV acquire against the real NATS Testcontainer.
        relays.get(0).relayCycle();
        if (!leases.get(0).isLeader()) {
            throw new IllegalStateException("node-0 failed to acquire initial leadership -- cannot run scenario");
        }

        // 2. Seed audit-critical outbox rows the (about to be killed) leader never drains.
        seedOutboxRows(env.dataSource(), SEEDED_ROW_COUNT);

        // 3. Kill replica 0: never call its relayCycle()/tryAcquireOrRenew() again. Its last KV
        //    revision now ages toward the bucket TTL with no renewal -- a hard-crash simulation.
        Instant killedAt = Instant.now();
        log.info("Simulated relay leader crash — node-0 will not renew its lease again");

        // 4. Poll standby replicas until exactly one acquires the lease.
        Instant recoveredAt = null;
        int newLeaderIndex = -1;
        Instant deadline = killedAt.plus(ttl).plus(SAFETY_MARGIN);
        while (recoveredAt == null && Instant.now().isBefore(deadline)) {
            for (int i = 1; i < engineNodeReplicaCount; i++) {
                relays.get(i).relayCycle();
                if (leases.get(i).isLeader()) {
                    recoveredAt = Instant.now();
                    newLeaderIndex = i;
                    break;
                }
            }
            if (recoveredAt == null) {
                sleepQuietly(POLL_INTERVAL);
            }
        }

        if (recoveredAt == null) {
            int stillPending = countOutboxRows(env.dataSource());
            log.error("Relay failover did NOT recover within TTL+safety margin — RTO breach",
                    kv("lease_ttl", ttl), kv("safety_margin", SAFETY_MARGIN));
            return new RelayFailoverReport(ttl.plus(SAFETY_MARGIN), ttl, false, SEEDED_ROW_COUNT, stillPending);
        }

        Duration timeToRecover = Duration.between(killedAt, recoveredAt);

        // 5. Drain remaining rows under the new leader (bounded retries -- one relayCycle() call
        //    should already drain this scenario's small seeded batch in a single pass).
        for (int attempt = 0; attempt < DRAIN_ATTEMPTS && countOutboxRows(env.dataSource()) > 0; attempt++) {
            relays.get(newLeaderIndex).relayCycle();
        }

        int rowsRemaining = countOutboxRows(env.dataSource());
        // See class Javadoc QA-FINDING: TTL-expiry recovery cannot happen strictly before `ttl`
        // elapses, so the bound this measurement can honestly assert includes one POLL_INTERVAL
        // of detection latency (the only slack this harness itself introduces).
        boolean recoveredWithinTtl = timeToRecover.compareTo(ttl.plus(POLL_INTERVAL)) <= 0;

        log.info("Relay failover measured -- timeToRecover={} leaseTtl={} recoveredWithinTtl={} rowsRemaining={}",
                timeToRecover, ttl, recoveredWithinTtl, rowsRemaining);

        return new RelayFailoverReport(timeToRecover, ttl, recoveredWithinTtl, SEEDED_ROW_COUNT, rowsRemaining);
    }

    private void seedOutboxRows(DataSource dataSource, int count) {
        PseudonymTokenGenerator tokenGenerator = new PseudonymTokenGenerator();
        HistoryClassificationProperties classification = new HistoryClassificationProperties();
        CompactHistoryOutboxWriter writer =
                new CompactHistoryOutboxWriter(dataSource, tokenGenerator, classification, null);
        for (int i = 0; i < count; i++) {
            UserOperationLogEntryEventEntity event = new UserOperationLogEntryEventEntity();
            event.setId(UUID.randomUUID().toString());
            event.setEventType("UserOperationLog");
            event.setProcessInstanceId("relay-failover-proc-" + i);
            event.setUserId("bench-user");
            try (Connection connection = dataSource.getConnection()) {
                connection.setAutoCommit(false);
                writer.write(event, HistoryClassNames.OP_LOG, ENGINE_ID, connection);
                connection.commit();
            } catch (SQLException e) {
                throw new IllegalStateException("Failed to seed compact_history_outbox row for failover scenario", e);
            }
        }
    }

    private int countOutboxRows(DataSource dataSource) {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement stmt =
                     connection.prepareStatement("SELECT count(*) FROM compact_history_outbox WHERE engine_id = ?")) {
            stmt.setString(1, ENGINE_ID);
            try (ResultSet rs = stmt.executeQuery()) {
                rs.next();
                return rs.getInt(1);
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to count compact_history_outbox rows", e);
        }
    }

    private static void sleepQuietly(Duration duration) {
        try {
            Thread.sleep(duration.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while polling for relay failover recovery", e);
        }
    }
}
