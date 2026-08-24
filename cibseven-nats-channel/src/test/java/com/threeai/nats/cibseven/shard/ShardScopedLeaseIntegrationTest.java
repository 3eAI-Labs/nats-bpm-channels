package com.threeai.nats.cibseven.shard;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;

import com.threeai.nats.core.jetstream.JetStreamKvManager;
import com.threeai.nats.core.jetstream.SweepLeaderLease;

import io.nats.client.Connection;
import io.nats.client.Nats;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * docs/13 G4 saha bulgusu (2026-08-24): filo-global sweep kirasi, liderligi bir shard'a
 * verip DIGER shard'in oksuzlerini supurulmez birakti (100k'lik kosuda 1 instance sonsuz
 * takildi). Duzeltme: DB-basina liderlik kiralari sharded modda shard-scoped kimlik
 * kullanir — bu test IKI shard'in AYNI bucket'ta AYNI ANDA lider olabildigini kanitlar.
 */
@Testcontainers
class ShardScopedLeaseIntegrationTest {

    @Container
    static GenericContainer<?> natsContainer = new GenericContainer<>("nats:2.10-alpine")
            .withCommand("--jetstream")
            .withExposedPorts(4222);

    private Connection connection;

    @BeforeEach
    void setUp() throws Exception {
        connection = Nats.connect("nats://" + natsContainer.getHost() + ":"
                + natsContainer.getMappedPort(4222));
    }

    @AfterEach
    void tearDown() throws Exception {
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    void shardScopedLeaseIds_bothShardsLeadSimultaneously_globalIdWouldNot() throws Exception {
        JetStreamKvManager kvManager = new JetStreamKvManager();
        kvManager.ensureBucket("a2-sweep-leader", Duration.ofSeconds(30), 1, connection);

        // the FIX: per-shard lease identity -> distinct keys in the SHARED bucket
        SweepLeaderLease shard0 = new SweepLeaderLease(connection.jetStream(), kvManager,
                connection, "cibseven-s0", "node-A", Duration.ofSeconds(30));
        SweepLeaderLease shard1 = new SweepLeaderLease(connection.jetStream(), kvManager,
                connection, "cibseven-s1", "node-B", Duration.ofSeconds(30));

        assertThat(shard0.tryAcquireOrRenew()).isTrue();
        assertThat(shard1.tryAcquireOrRenew()).isTrue(); // both lead — each sweeps its own DB

        // the BUG's shape: one GLOBAL identity -> second node cannot lead, its shard starves
        SweepLeaderLease globalA = new SweepLeaderLease(connection.jetStream(), kvManager,
                connection, "cibseven", "node-A", Duration.ofSeconds(30));
        SweepLeaderLease globalB = new SweepLeaderLease(connection.jetStream(), kvManager,
                connection, "cibseven", "node-B", Duration.ofSeconds(30));
        assertThat(globalA.tryAcquireOrRenew()).isTrue();
        assertThat(globalB.tryAcquireOrRenew()).isFalse(); // exactly the observed starvation
    }
}
