package com.threeai.nats.core.shard;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;

import io.nats.client.Connection;
import io.nats.client.JetStreamManagement;
import io.nats.client.Nats;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.DiscardPolicy;
import io.nats.client.api.RetentionPolicy;
import io.nats.client.api.StreamConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/** docs/13 §2.7 slice 3 — the HARD boot gate against a real broker. */
@Testcontainers
class ShardBootstrapValidatorIntegrationTest {

    @Container
    static GenericContainer<?> natsContainer = new GenericContainer<>("nats:2.10-alpine")
            .withCommand("--jetstream")
            .withExposedPorts(4222);

    private Connection connection;
    private JetStreamManagement jsm;
    private int suffix; // unique stream names per test — container is class-shared

    @BeforeEach
    void setUp() throws Exception {
        String url = "nats://" + natsContainer.getHost() + ":" + natsContainer.getMappedPort(4222);
        connection = Nats.connect(url);
        jsm = connection.jetStreamManagement();
        suffix = (int) (System.nanoTime() % 1_000_000);
        // clean slate: remove every stream previous tests left behind
        for (String stream : jsm.getStreamNames()) {
            jsm.deleteStream(stream);
        }
    }

    @AfterEach
    void tearDown() throws Exception {
        if (connection != null) {
            connection.close();
        }
    }

    private void addShardStream(int shard, java.util.function.UnaryOperator<StreamConfiguration.Builder> tweak)
            throws Exception {
        StreamConfiguration.Builder builder = StreamConfiguration.builder()
                .name("SHARD-S" + shard + "-" + suffix)
                .subjects("shard." + shard + ".>")
                .retentionPolicy(RetentionPolicy.WorkQueue)
                .discardPolicy(DiscardPolicy.New)
                .maxBytes(64 * 1024 * 1024)
                .duplicateWindow(Duration.ofSeconds(200));
        jsm.addStream(tweak.apply(builder).build());
    }

    private ShardBootstrapValidator validator(int shardCount) {
        // horizon example: 30s ackWait × (5+1) = 180s < 200s window
        return new ShardBootstrapValidator(connection, new ShardTopology(shardCount, 0), 180);
    }

    @Test
    void wellProvisionedTopology_passes() throws Exception {
        addShardStream(0, b -> b);
        addShardStream(1, b -> b);

        assertThatCode(() -> validator(2).validate()).doesNotThrowAnyException();
    }

    @Test
    void missingShardStream_failsWithClearCode() throws Exception {
        addShardStream(0, b -> b);
        // shard 1 stream deliberately absent

        assertThatThrownBy(() -> validator(2).validate())
                .hasMessageContaining("SHARD-BOOT-STREAM-MISSING")
                .hasMessageContaining("shard.1.>");
    }

    @Test
    void sharedStreamAcrossShards_rejected_blastRadius() throws Exception {
        // the anti-pattern the per-shard split exists to prevent: one stream, all shards
        jsm.addStream(StreamConfiguration.builder()
                .name("SHARD-ALL-" + suffix)
                .subjects("shard.>")
                .retentionPolicy(RetentionPolicy.WorkQueue)
                .discardPolicy(DiscardPolicy.New)
                .maxBytes(64 * 1024 * 1024)
                .duplicateWindow(Duration.ofSeconds(200))
                .build());

        assertThatThrownBy(() -> validator(2).validate())
                .hasMessageContaining("SHARD-BOOT-STREAM-SHARED");
    }

    @Test
    void wrongDiscardPolicy_rejected() throws Exception {
        addShardStream(0, b -> b.discardPolicy(DiscardPolicy.Old));
        addShardStream(1, b -> b);

        assertThatThrownBy(() -> validator(2).validate())
                .hasMessageContaining("SHARD-BOOT-STREAM-PROPS")
                .hasMessageContaining("discard");
    }

    @Test
    void windowBelowRedeliveryHorizon_rejected() throws Exception {
        addShardStream(0, b -> b.duplicateWindow(Duration.ofSeconds(60))); // < 180s horizon
        addShardStream(1, b -> b);

        assertThatThrownBy(() -> validator(2).validate())
                .hasMessageContaining("SHARD-BOOT-STREAM-PROPS")
                .hasMessageContaining("duplicate_window");
    }

    @Test
    void legacyDurableStillPresent_rejected_activationSkipped() throws Exception {
        addShardStream(0, b -> b);
        addShardStream(1, b -> b);
        jsm.addStream(StreamConfiguration.builder()
                .name("JOBS-REPLY-" + suffix)
                .subjects("jobs.*.reply")
                .retentionPolicy(RetentionPolicy.WorkQueue)
                .build());
        jsm.addOrUpdateConsumer("JOBS-REPLY-" + suffix, ConsumerConfiguration.builder()
                .durable("a2-completion-orderTopic")
                .filterSubject("jobs.orderTopic.reply")
                .deliverSubject("push.a2c." + suffix)  // push consumer, like the real bridge
                .deliverGroup("a2-completion-orderTopic")
                .build());

        assertThatThrownBy(() -> validator(2).validate())
                .hasMessageContaining("SHARD-BOOT-ACTIVATION-SKIPPED")
                .hasMessageContaining("a2-completion-orderTopic");
    }

    @Test
    void brokerUnreachable_throwsClearError() throws Exception {
        Connection closed = Nats.connect("nats://" + natsContainer.getHost() + ":"
                + natsContainer.getMappedPort(4222));
        closed.close();
        ShardBootstrapValidator validator = new ShardBootstrapValidator(
                closed, new ShardTopology(2, 0), 180);

        assertThatThrownBy(validator::validate)
                .hasMessageContaining("SHARD-BOOT-BROKER");
    }

    @Test
    void shardPrefixIsDotBounded_probeDoesNotMatchForeignSubjects() throws Exception {
        // a stream on 'sharding.metrics' must NOT satisfy shard-stream resolution
        jsm.addStream(StreamConfiguration.builder()
                .name("FOREIGN-" + suffix).subjects("sharding.metrics").build());
        addShardStream(0, b -> b);
        addShardStream(1, b -> b);

        assertThatCode(() -> validator(2).validate()).doesNotThrowAnyException();
        assertThat(jsm.getStreamNames("shard.0.>")).hasSize(1);
    }
}
