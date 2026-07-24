package com.threeai.nats.bench.outbound;

import static net.logstash.logback.argument.StructuredArguments.kv;

import java.nio.charset.StandardCharsets;
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
import com.threeai.nats.bench.history.RelayFailoverReport;
import com.threeai.nats.core.db.SqlMigrationRunner;
import com.threeai.nats.core.jetstream.JetStreamKvManager;
import com.threeai.nats.core.jetstream.JetStreamStreamManager;
import com.threeai.nats.core.jetstream.SweepLeaderLease;
import com.threeai.nats.core.outbound.OutboundMessageDraft;
import com.threeai.nats.core.outbound.OutboundMessageOutboxProperties;
import com.threeai.nats.core.outbound.OutboundMessageOutboxWriter;
import com.threeai.nats.core.outbound.OutboundMessageRelay;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code outbound-relay-leader} failover RTO/RPO measurement — basamak-4 mirror of {@code
 * RelayFailoverBenchScenario} (basamak-2's real-measurement precedent for {@code
 * history-relay-leader}), applied to {@code OutboundMessageRelay}/{@code outbound_message_outbox}
 * (docs/09-outbound-handoff.md D-A'/D-F'). Reuses the SAME {@link RelayFailoverReport} shape and
 * the SAME procedure: {@code engineNodeReplicaCount} in-process {@link SweepLeaderLease}/{@link
 * OutboundMessageRelay} pairs race the SAME real NATS JetStream KV bucket; the current leader is
 * "killed" (never renewed again) with audit-critical rows in flight; a standby's TTL-expiry-driven
 * takeover is measured, and post-failover the seeded rows must be fully drained (RPO=0).
 *
 * <p>Engine-neutral (no camunda/cadenzaflow-fork types) — {@code OutboundMessageRelay} itself lives
 * once in {@code nats-core} and is parameterized by {@code engineId} only, so this scenario proves
 * the invariant for BOTH engine families without any per-engine duplication (see {@code
 * OutboundMessageRelay}'s own class Javadoc).
 */
public class OutboundRelayFailoverBenchScenario {

    private static final Logger log = LoggerFactory.getLogger(OutboundRelayFailoverBenchScenario.class);
    private static final String ENGINE_ID = "camunda";
    private static final int SEEDED_ROW_COUNT = 5;
    private static final Duration POLL_INTERVAL = Duration.ofMillis(500);
    private static final Duration SAFETY_MARGIN = Duration.ofSeconds(30);
    private static final int DRAIN_ATTEMPTS = 5;

    public RelayFailoverReport run(BenchEnvironment env, int engineNodeReplicaCount) {
        if (engineNodeReplicaCount < 2) {
            throw new IllegalArgumentException(
                    "OutboundRelayFailoverBenchScenario needs >=2 engine-node replicas to observe a failover, got "
                            + engineNodeReplicaCount);
        }

        SqlMigrationRunner.applyClasspathScript(env.dataSource(), "db/migration/outbound/V1__outbound_message_outbox.sql");
        // BenchEnvironment only provisions the A2 job/reply/DLQ streams (03_classes/5_bench.md
        // §1) -- OutboundMessageRelay publishes to "events.<engineId>.<type>.<processInstanceId>",
        // which has no matching stream there yet; without one, jetStream.publish() fails with "no
        // responders"/"no stream matches subject" and every relayCycle() would (correctly) retain
        // the row forever, masquerading as an RPO breach that is really just missing provisioning.
        new JetStreamStreamManager().ensureStream("BENCH-EVENTS", "events.>", env.natsConnection());

        OutboundMessageOutboxProperties properties = new OutboundMessageOutboxProperties(); // production default: 30s cycle
        Duration ttl = Duration.ofSeconds(2 * properties.getRelayCyclePeriodSeconds()); // ADR-0002 precedent -- 60s

        String bucket = "outbound-relay-leader-failover-" + UUID.randomUUID();
        JetStreamKvManager kvManager = new JetStreamKvManager();
        kvManager.ensureBucket(bucket, ttl, 1, env.natsConnection());

        List<SweepLeaderLease> leases = new ArrayList<>(engineNodeReplicaCount);
        List<OutboundMessageRelay> relays = new ArrayList<>(engineNodeReplicaCount);
        for (int i = 0; i < engineNodeReplicaCount; i++) {
            SweepLeaderLease lease = new SweepLeaderLease(env.jetStream(), kvManager, env.natsConnection(),
                    bucket, "relay-leader.", ENGINE_ID, "node-" + i, ttl);
            leases.add(lease);
            relays.add(new OutboundMessageRelay(env.dataSource(), env.jetStream(), lease, properties, null, ENGINE_ID));
        }

        // 1. Replica 0 becomes leader -- genuine KV acquire against the real NATS Testcontainer.
        relays.get(0).relayCycle();
        if (!leases.get(0).isLeader()) {
            throw new IllegalStateException("node-0 failed to acquire initial leadership -- cannot run scenario");
        }

        // 2. Seed audit-critical outbox rows the (about to be killed) leader never drains.
        seedOutboxRows(env.dataSource(), SEEDED_ROW_COUNT);

        // 3. Kill replica 0: never call its relayCycle()/tryAcquireOrRenew() again -- its last KV
        //    revision now ages toward the bucket TTL with no renewal (hard-crash simulation).
        Instant killedAt = Instant.now();
        log.info("Simulated outbound relay leader crash — node-0 will not renew its lease again");

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
            log.error("Outbound relay failover did NOT recover within TTL+safety margin — RTO breach",
                    kv("lease_ttl", ttl), kv("safety_margin", SAFETY_MARGIN));
            return new RelayFailoverReport(ttl.plus(SAFETY_MARGIN), ttl, false, SEEDED_ROW_COUNT, stillPending);
        }

        Duration timeToRecover = Duration.between(killedAt, recoveredAt);

        // 5. Drain remaining rows under the new leader (bounded retries).
        for (int attempt = 0; attempt < DRAIN_ATTEMPTS && countOutboxRows(env.dataSource()) > 0; attempt++) {
            relays.get(newLeaderIndex).relayCycle();
        }

        int rowsRemaining = countOutboxRows(env.dataSource());
        boolean recoveredWithinTtl = timeToRecover.compareTo(ttl.plus(POLL_INTERVAL)) <= 0;

        log.info("Outbound relay failover measured -- timeToRecover={} leaseTtl={} recoveredWithinTtl={} rowsRemaining={}",
                timeToRecover, ttl, recoveredWithinTtl, rowsRemaining);

        return new RelayFailoverReport(timeToRecover, ttl, recoveredWithinTtl, SEEDED_ROW_COUNT, rowsRemaining);
    }

    private void seedOutboxRows(DataSource dataSource, int count) {
        OutboundMessageOutboxWriter writer = new OutboundMessageOutboxWriter(null);
        for (int i = 0; i < count; i++) {
            OutboundMessageDraft draft = new OutboundMessageDraft(ENGINE_ID, "order.created",
                    "outbound-relay-failover-proc-" + i, "bench-biz-" + i, "bench-trace-" + i,
                    "events." + ENGINE_ID + ".order.created.outbound-relay-failover-proc-" + i,
                    "{\"bench\":true}".getBytes(StandardCharsets.UTF_8));
            try (Connection connection = dataSource.getConnection()) {
                connection.setAutoCommit(false);
                writer.write(connection, draft);
                connection.commit();
            } catch (SQLException e) {
                throw new IllegalStateException("Failed to seed outbound_message_outbox row for failover scenario", e);
            }
        }
    }

    private int countOutboxRows(DataSource dataSource) {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement stmt =
                     connection.prepareStatement("SELECT count(*) FROM outbound_message_outbox WHERE engine_id = ?")) {
            stmt.setString(1, ENGINE_ID);
            try (ResultSet rs = stmt.executeQuery()) {
                rs.next();
                return rs.getInt(1);
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to count outbound_message_outbox rows", e);
        }
    }

    private static void sleepQuietly(Duration duration) {
        try {
            Thread.sleep(duration.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while polling for outbound relay failover recovery", e);
        }
    }
}
