package com.threeai.nats.camunda.shard;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

import com.threeai.nats.camunda.eventing.EventingPayloadMapper;
import com.threeai.nats.camunda.inbound.JetStreamMessageCorrelationSubscriber;
import com.threeai.nats.camunda.inbound.SubscriptionConfig;
import com.threeai.nats.core.dlq.DlqPublisher;
import com.threeai.nats.core.headers.BpmHeaders;
import com.threeai.nats.core.history.HistoryEventEnvelope;
import com.threeai.nats.core.metrics.NatsChannelMetrics;
import com.threeai.nats.core.shard.ShardBootstrapValidator;
import com.threeai.nats.core.shard.ShardRouteConfig;
import com.threeai.nats.core.shard.ShardRouter;
import com.threeai.nats.core.shard.ShardTopology;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.nats.client.Connection;
import io.nats.client.JetStreamManagement;
import io.nats.client.Nats;
import io.nats.client.api.DiscardPolicy;
import io.nats.client.api.RetentionPolicy;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.impl.NatsMessage;
import org.camunda.bpm.engine.ProcessEngine;
import org.camunda.bpm.engine.ProcessEngineConfiguration;
import org.camunda.bpm.engine.impl.cfg.ProcessEngineConfigurationImpl;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * docs/13 §5 slice 7 — the 2-shard acceptance: TWO real engines (own H2 each), one real
 * broker, operator-provisioned per-shard streams, the real validator, the real router, and
 * shard-rewritten correlation subscribers. Frozen vectors: "ORD-123"→shard 1, "a"→shard 0.
 */
@Testcontainers
class ShardedTwoShardAcceptanceTest {

    private static final String BPMN = """
            <?xml version="1.0" encoding="UTF-8"?>
            <definitions xmlns="http://www.omg.org/spec/BPMN/20100524/MODEL"
                         xmlns:camunda="http://camunda.org/schema/1.0/bpmn"
                         targetNamespace="shardacc">
              <message id="m1" name="OrderPaid"/>
              <process id="orderProc" isExecutable="true" camunda:historyTimeToLive="P1D">
                <startEvent id="s"/>
                <sequenceFlow id="f1" sourceRef="s" targetRef="catch"/>
                <intermediateCatchEvent id="catch">
                  <messageEventDefinition messageRef="m1"/>
                </intermediateCatchEvent>
                <sequenceFlow id="f2" sourceRef="catch" targetRef="done"/>
                <userTask id="done"/>
                <sequenceFlow id="f3" sourceRef="done" targetRef="e"/>
                <endEvent id="e"/>
              </process>
            </definitions>
            """;

    @Container
    static GenericContainer<?> natsContainer = new GenericContainer<>("nats:2.10-alpine")
            .withCommand("--jetstream").withExposedPorts(4222);

    private Connection connection;
    private JetStreamManagement jsm;
    private ProcessEngine engine0;
    private ProcessEngine engine1;
    private JetStreamMessageCorrelationSubscriber subscriber0;
    private JetStreamMessageCorrelationSubscriber subscriber1;
    private ShardRouter router;
    private SimpleMeterRegistry registry;

    @BeforeEach
    void setUp() throws Exception {
        connection = Nats.connect("nats://" + natsContainer.getHost() + ":"
                + natsContainer.getMappedPort(4222));
        jsm = connection.jetStreamManagement();
        for (String stream : jsm.getStreamNames()) {
            jsm.deleteStream(stream);
        }
        // --- activation step 3: operator provisions per-shard streams + source + shard DLQ
        provision("SHARD-S0", "shard.0.>", 32 * 1024 * 1024);
        provision("SHARD-S1", "shard.1.>", 32 * 1024 * 1024);
        jsm.addStream(StreamConfiguration.builder()
                .name("EVT").subjects("evt.>").maxBytes(32 * 1024 * 1024).build());
        jsm.addStream(StreamConfiguration.builder()
                .name("DLQ-SHARD").subjects("dlq.shard.>").maxBytes(8 * 1024 * 1024).build());

        registry = new SimpleMeterRegistry();
        engine0 = engine(0);
        engine1 = engine(1);
    }

    private void provision(String name, String subject, long maxBytes) throws Exception {
        jsm.addStream(StreamConfiguration.builder()
                .name(name).subjects(subject)
                .retentionPolicy(RetentionPolicy.WorkQueue)
                .discardPolicy(DiscardPolicy.New)
                .maxBytes(maxBytes)
                .duplicateWindow(Duration.ofSeconds(200))
                .build());
    }

    private ProcessEngine engine(int shardId) {
        ProcessEngineConfigurationImpl configuration = (ProcessEngineConfigurationImpl)
                ProcessEngineConfiguration.createStandaloneInMemProcessEngineConfiguration()
                        .setJdbcUrl("jdbc:h2:mem:shard-acc-" + shardId + "-" + System.nanoTime()
                                + ";DB_CLOSE_DELAY=-1")
                        .setDatabaseSchemaUpdate(ProcessEngineConfiguration.DB_SCHEMA_UPDATE_TRUE)
                        .setJobExecutorActivate(false);
        configuration.setCustomPreBPMNParseListeners(List.of(
                new ShardBirthGuardParseListener(new ShardTopology(2, shardId))));
        ProcessEngine engine = configuration.buildProcessEngine();
        engine.getRepositoryService().createDeployment().addString("p.bpmn20.xml", BPMN).deploy();
        return engine;
    }

    private void startTopology() throws Exception {
        // validator = the hard gate, on the provisioned topology (both shards' view)
        new ShardBootstrapValidator(connection, new ShardTopology(2, 0), 180).validate();
        new ShardBootstrapValidator(connection, new ShardTopology(2, 1), 180).validate();

        subscriber0 = subscriber(engine0, 0);
        subscriber1 = subscriber(engine1, 1);
        router = newRouter();
        router.subscribe();
    }

    private ShardRouter newRouter() throws Exception {
        NatsChannelMetrics metrics = new NatsChannelMetrics(registry);
        return new ShardRouter(connection, connection.jetStream(), new ShardTopology(2, 0),
                List.of(new ShardRouteConfig("evt.order.paid", "orderId")),
                EventingPayloadMapper::topLevelScalarAsText,
                new DlqPublisher(connection.jetStream(), connection, metrics), metrics, 5);
    }

    private JetStreamMessageCorrelationSubscriber subscriber(ProcessEngine engine, int shardId)
            throws Exception {
        SubscriptionConfig config = new SubscriptionConfig();
        config.setSubject("evt.order.paid");
        config.setMessageName("OrderPaid");
        config.setJetstream(true);
        config.setBusinessKeyHeader(BpmHeaders.BUSINESS_KEY);
        ShardSubscriptionRewriter.rewrite(config, new ShardTopology(2, shardId));
        JetStreamMessageCorrelationSubscriber subscriber = new JetStreamMessageCorrelationSubscriber(
                connection, connection.jetStream(), engine.getRuntimeService(), config,
                new NatsChannelMetrics(registry),
                new DlqPublisher(connection.jetStream(), connection, new NatsChannelMetrics(registry)));
        subscriber.subscribe();
        return subscriber;
    }

    @AfterEach
    void tearDown() throws Exception {
        if (router != null) {
            router.unsubscribe();
        }
        if (subscriber0 != null) {
            subscriber0.unsubscribe();
        }
        if (subscriber1 != null) {
            subscriber1.unsubscribe();
        }
        if (engine0 != null) {
            engine0.close();
        }
        if (engine1 != null) {
            engine1.close();
        }
        if (connection != null) {
            connection.close();
        }
    }

    private void publishOrderPaid(String businessKey) throws Exception {
        connection.jetStream().publish(NatsMessage.builder()
                .subject("evt.order.paid")
                .data(("{\"orderId\":\"" + businessKey + "\",\"amount\":9.5}")
                        .getBytes(StandardCharsets.UTF_8))
                .build());
    }

    private long advanced(ProcessEngine engine) {
        return engine.getTaskService().createTaskQuery().taskDefinitionKey("done").count();
    }

    @Test
    void correlation_reachesOnlyOwningShard_wrongShardNeverAdvances() throws Exception {
        startTopology();
        engine1.getRuntimeService().startProcessInstanceByKey("orderProc", "ORD-123"); // s1
        engine0.getRuntimeService().startProcessInstanceByKey("orderProc", "a");       // s0

        publishOrderPaid("ORD-123");

        awaitTrue(() -> advanced(engine1) == 1, 15000, "owning shard (1) to advance");
        Thread.sleep(500); // give a misroute a chance to surface
        assertThat(advanced(engine0)).isZero(); // wrong shard: untouched
        assertThat(registry.counter("nats.shard.routed",
                "subject", "evt.order.paid", "target_shard", "1").count()).isEqualTo(1.0);

        publishOrderPaid("a");
        awaitTrue(() -> advanced(engine0) == 1, 15000, "shard 0's own message to arrive");
        assertThat(advanced(engine1)).isEqualTo(1); // still exactly one
    }

    @Test
    void routerKillRestart_messagesSurvive_exactlyOnce() throws Exception {
        startTopology();
        engine1.getRuntimeService().startProcessInstanceByKey("orderProc", "ORD-123");

        router.unsubscribe(); // the fleet's routers all die
        publishOrderPaid("ORD-123"); // message waits durably in EVT

        Thread.sleep(1000);
        assertThat(advanced(engine1)).isZero(); // nothing routed while dead

        router = newRouter();
        router.subscribe(); // restart -> durable consumer resumes

        awaitTrue(() -> advanced(engine1) == 1, 15000, "post-restart delivery");
        Thread.sleep(500);
        assertThat(advanced(engine1)).isEqualTo(1); // exactly once, no double correlation
    }

    @Test
    void capFull_custodyKept_recoveryDeliversExactlyOnce() throws Exception {
        // shrink shard-1's stream to a tiny cap and fill it BEFORE the topology starts
        jsm.deleteStream("SHARD-S1");
        provision("SHARD-S1", "shard.1.>", 2 * 1024);
        // fill TIGHT: shrink filler size on each rejection until even 1 byte is refused,
        // so the router's (small) republish cannot squeeze under the cap
        int fillerSize = 512;
        int guard = 0;
        while (fillerSize >= 1 && guard++ < 200) {
            try {
                connection.jetStream().publish("shard.1.filler", new byte[fillerSize]);
            } catch (Exception rejected) {
                fillerSize /= 2; // discard=new rejects visibly, never silently
            }
        }
        startTopology();
        engine1.getRuntimeService().startProcessInstanceByKey("orderProc", "ORD-123");

        publishOrderPaid("ORD-123"); // router's republish will be REJECTED at the cap

        awaitTrue(() -> registry.counter("nats.shard.publish_reject",
                "subject", "evt.order.paid").count() >= 1.0, 15000, "custody reject counter");
        assertThat(advanced(engine1)).isZero(); // not lost, not delivered — waiting

        jsm.purgeStream("SHARD-S1"); // operator frees the cap

        awaitTrue(() -> advanced(engine1) == 1, 30000, "redelivery to route after recovery");
        Thread.sleep(500);
        assertThat(advanced(engine1)).isEqualTo(1); // derived-Msg-Id dedup: exactly once
    }

    @Test
    void keylessMessage_toShardDlq_sourceAcked_neverEscalated() throws Exception {
        startTopology();

        connection.jetStream().publish(NatsMessage.builder()
                .subject("evt.order.paid")
                .data("{\"note\":\"no key here\"}".getBytes(StandardCharsets.UTF_8))
                .build());

        awaitTrue(() -> {
            try {
                return jsm.getStreamInfo("DLQ-SHARD").getStreamState().getMsgCount() == 1;
            } catch (Exception e) {
                return false;
            }
        }, 15000, "keyless message to land in dlq.shard.");
        assertThat(registry.counter("nats.shard.key_missing",
                "subject", "evt.order.paid").count()).isEqualTo(1.0);
        assertThat(advanced(engine0) + advanced(engine1)).isZero();
    }

    @Test
    void historyIngestSurface_isShardAgnostic_bothShardsOneStream() throws Exception {
        // Y-8 unified-surface hook: history subjects carry NO shard token — both shards'
        // engines publish into the SAME history stream the (single) projection consumes.
        jsm.addStream(StreamConfiguration.builder()
                .name("HISTORY").subjects("history.>").maxBytes(8 * 1024 * 1024).build());
        HistoryEventEnvelope fromShard0 = new HistoryEventEnvelope("camunda", "ACT_HI_PROCINST",
                "start", "h1", "pi-0", "a", 0, Instant.now(), "{}");
        HistoryEventEnvelope fromShard1 = new HistoryEventEnvelope("camunda", "ACT_HI_PROCINST",
                "start", "h2", "pi-1", "ORD-123", 0, Instant.now(), "{}");

        connection.jetStream().publish(fromShard0.subject(), "{}".getBytes(StandardCharsets.UTF_8));
        connection.jetStream().publish(fromShard1.subject(), "{}".getBytes(StandardCharsets.UTF_8));

        assertThat(fromShard0.subject()).doesNotContain("shard.");
        assertThat(jsm.getStreamInfo("HISTORY").getStreamState().getMsgCount()).isEqualTo(2);
    }

    @Test
    void validator_greenOnProvisionedTopology() {
        assertThatCode(() -> new ShardBootstrapValidator(
                connection, new ShardTopology(2, 0), 180).validate())
                .doesNotThrowAnyException();
    }

    private void awaitTrue(java.util.function.BooleanSupplier condition, long timeoutMs, String what) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            try {
                Thread.sleep(150);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        throw new AssertionError("Timed out waiting for: " + what);
    }
}
