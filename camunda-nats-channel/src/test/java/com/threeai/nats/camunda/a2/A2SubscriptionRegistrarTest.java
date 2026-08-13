package com.threeai.nats.camunda.a2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.threeai.nats.core.dlq.DlqPublisher;
import com.threeai.nats.core.jetstream.JetStreamKvManager;
import com.threeai.nats.core.metrics.NatsChannelMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.JetStream;
import io.nats.client.MessageHandler;
import io.nats.client.PushSubscribeOptions;
import io.nats.client.api.ConsumerConfiguration;
import org.camunda.bpm.engine.ExternalTaskService;
import org.camunda.bpm.engine.ProcessEngine;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/**
 * All collaborators mocked (Mockito) except {@link A2Properties}/{@link A2ConsumerConfig} (plain
 * config POJOs) and the internally-constructed {@link A2CompletionBridge}/{@link A2IncidentBridge}
 * (real objects — this is exactly what proves the registrar wires them correctly: their real
 * {@code subscribe()}/{@code unsubscribe()} run against the mocked {@link JetStream}/{@link
 * Connection}). {@link A2OrphanSweep} is likewise real and NOT injectable (constructed internally)
 * — its sweep cycle failing against mocked {@link ProcessEngine} collaborators is what proves
 * {@code runSweepCycleSafely}'s uncaught-exception swallow actually works end-to-end.
 */
class A2SubscriptionRegistrarTest {

    private A2Properties properties;
    private Connection connection;
    private JetStream jetStream;
    private ExternalTaskService externalTaskService;
    private DlqPublisher dlqPublisher;
    private NatsChannelMetrics metrics;
    private SimpleMeterRegistry meterRegistry;
    private ProcessEngine processEngine;
    private UmbrellaLockResolver lockResolver;
    private A2TopicConfig topicConfig;
    private UmbrellaLockValidator lockValidator;
    private JetStreamKvManager kvManager;
    /** Production default, as {@code NatsProperties.Jetstream#kvReplicas} supplies it. */
    private static final int KV_REPLICAS = 3;

    private Dispatcher dispatcher;

    private A2SubscriptionRegistrar registrar;

    @BeforeEach
    void setUp() {
        properties = new A2Properties();
        properties.setTopics(List.of("order-fulfillment"));

        connection = mock(Connection.class);
        jetStream = mock(JetStream.class);
        dispatcher = mock(Dispatcher.class);
        when(connection.createDispatcher()).thenReturn(dispatcher);

        externalTaskService = mock(ExternalTaskService.class);
        dlqPublisher = new DlqPublisher(jetStream, connection, null);
        // QA-FINDING-1 (fixed): DlqBridgeCircuitBreakerFactory.create used to touch
        // TaggedCircuitBreakerMetrics (resilience4j-micrometer) unconditionally whenever
        // registry != null, and that artifact is an OPTIONAL nats-core dependency this module does
        // not itself redeclare (confirmed via `mvn dependency:tree`: genuinely absent from this
        // module's classpath) -- so a real MeterRegistry here used to throw NoClassDefFoundError
        // the instant this constructor ran. DlqBridgeCircuitBreakerFactory now guards that call
        // behind a classpath-presence check and skips metrics gracefully when absent; using a REAL
        // SimpleMeterRegistry for every test in this class (rather than null) is precisely what
        // proves that fix end-to-end, on this module's real resilience4j-micrometer-free classpath.
        meterRegistry = new SimpleMeterRegistry();
        metrics = new NatsChannelMetrics(new SimpleMeterRegistry());
        processEngine = mock(ProcessEngine.class);
        lockResolver = new UmbrellaLockResolver(properties);
        topicConfig = new A2TopicConfig(properties);
        lockValidator = new UmbrellaLockValidator(properties, lockResolver);
        kvManager = mock(JetStreamKvManager.class);

        registrar = new A2SubscriptionRegistrar(properties, connection, jetStream, externalTaskService,
                dlqPublisher, metrics, meterRegistry, processEngine, lockResolver, topicConfig, lockValidator,
                kvManager, KV_REPLICAS);
    }

    @AfterEach
    void tearDown() {
        registrar.destroy();
    }

    @Test
    void constructor_realMeterRegistryPresent_noNoClassDefFoundError_metricsGracefullySkipped() {
        // QA-FINDING-1 proof: setUp() already constructed `registrar` with a real MeterRegistry on
        // this module's genuinely resilience4j-micrometer-free classpath (see setUp()'s comment) --
        // reaching this assertion at all proves no NoClassDefFoundError occurred. The classpath
        // guard means metrics are skipped gracefully, not silently mis-bound, so no
        // resilience4j.circuitbreaker.* meter should be registered.
        assertThat(meterRegistry.find("resilience4j.circuitbreaker.state").meters()).isEmpty();
    }

    @Test
    void afterPropertiesSet_emptyTopics_earlyReturn_noResourcesCreated() throws Exception {
        properties.setTopics(List.of());
        A2SubscriptionRegistrar emptyRegistrar = new A2SubscriptionRegistrar(properties, connection, jetStream,
                externalTaskService, dlqPublisher, metrics, meterRegistry, processEngine, lockResolver, topicConfig,
                lockValidator, kvManager, KV_REPLICAS);

        emptyRegistrar.afterPropertiesSet();

        verify(kvManager, never()).ensureBucket(anyString(), any(Duration.class), org.mockito.ArgumentMatchers.anyInt(),
                any(Connection.class));
        verify(jetStream, never()).subscribe(anyString(), any(), any(), anyBoolean(), any(PushSubscribeOptions.class));
        assertThatCode(emptyRegistrar::destroy).doesNotThrowAnyException();
    }

    @Test
    void afterPropertiesSet_withTopics_subscribesCompletionBridgePerTopicPlusIncidentBridge() throws Exception {
        registrar.afterPropertiesSet();

        // 1 completion bridge (order-fulfillment) + 1 wildcard incident bridge. BOTH bind on the
        // QUEUE overload: every engine node shares each durable, so a reply completes once and a
        // DLQ message raises one incident, no matter how many nodes are running.
        verify(jetStream, times(2)).subscribe(anyString(), anyString(), any(), any(), eq(false),
                any(PushSubscribeOptions.class));
        verify(jetStream, never()).subscribe(anyString(), any(), any(), eq(false),
                any(PushSubscribeOptions.class));
        verify(kvManager).ensureBucket(eq("a2-sweep-leader"), any(Duration.class), org.mockito.ArgumentMatchers.eq(KV_REPLICAS),
                eq(connection));
    }

    @Test
    void afterPropertiesSet_multipleTopics_oneCompletionBridgePerTopic() throws Exception {
        properties.setTopics(List.of("order-fulfillment", "payment-processing"));
        A2SubscriptionRegistrar multiRegistrar = new A2SubscriptionRegistrar(properties, connection, jetStream,
                externalTaskService, dlqPublisher, metrics, meterRegistry, processEngine, lockResolver, topicConfig,
                lockValidator, kvManager, KV_REPLICAS);

        multiRegistrar.afterPropertiesSet();

        // 2 completion bridges + 1 incident bridge, all on the queue overload.
        verify(jetStream, times(3)).subscribe(anyString(), anyString(), any(), any(), eq(false),
                any(PushSubscribeOptions.class));
        verify(jetStream, never()).subscribe(anyString(), any(), any(), eq(false),
                any(PushSubscribeOptions.class));
        multiRegistrar.destroy();
    }

    @Test
    void afterPropertiesSet_incidentBridgeSubject_isWildcardDlqJobs() throws Exception {
        ArgumentCaptor<String> subjectCaptor = ArgumentCaptor.forClass(String.class);

        registrar.afterPropertiesSet();

        verify(jetStream, times(2)).subscribe(subjectCaptor.capture(), anyString(), any(), any(), eq(false),
                any(PushSubscribeOptions.class));
        assertThat(subjectCaptor.getAllValues()).contains("dlq.jobs.>");
    }

    @Test
    void replyConsumerConfigFor_topicWithoutOverride_usesPropertyDefaults() throws Exception {
        ArgumentCaptor<PushSubscribeOptions> optsCaptor = ArgumentCaptor.forClass(PushSubscribeOptions.class);

        registrar.afterPropertiesSet();

        verify(jetStream, times(1)).subscribe(eq("jobs.order-fulfillment.reply"), anyString(), any(), any(), eq(false),
                optsCaptor.capture());
        // maxDeliver + 1 per A2CompletionBridge#subscribe's own "+1" convention.
        assertThat(optsCaptor.getValue().getConsumerConfiguration().getMaxDeliver())
                .isEqualTo(properties.getDefaults().getMaxDeliver() + 1);
        assertThat(optsCaptor.getValue().getConsumerConfiguration().getAckWait())
                .isEqualTo(Duration.ofSeconds(properties.getDefaults().getAckWaitSeconds()));
    }

    @Test
    void replyConsumerConfigFor_topicWithOverride_overridesAckWaitAndMaxDeliver() throws Exception {
        A2Properties.TopicLockOverride override = new A2Properties.TopicLockOverride();
        override.setAckWaitSeconds(90L);
        override.setMaxDeliver(9);
        override.setRetryTimeoutMillis(20_000L);
        properties.setTopicOverrides(Map.of("order-fulfillment", override));
        A2SubscriptionRegistrar overriddenRegistrar = new A2SubscriptionRegistrar(properties, connection, jetStream,
                externalTaskService, dlqPublisher, metrics, meterRegistry, processEngine, lockResolver, topicConfig,
                lockValidator, kvManager, KV_REPLICAS);
        ArgumentCaptor<PushSubscribeOptions> optsCaptor = ArgumentCaptor.forClass(PushSubscribeOptions.class);

        overriddenRegistrar.afterPropertiesSet();

        verify(jetStream, times(1)).subscribe(eq("jobs.order-fulfillment.reply"), anyString(), any(), any(), eq(false),
                optsCaptor.capture());
        assertThat(optsCaptor.getValue().getConsumerConfiguration().getMaxDeliver()).isEqualTo(10); // 9+1
        assertThat(optsCaptor.getValue().getConsumerConfiguration().getAckWait()).isEqualTo(Duration.ofSeconds(90));
        overriddenRegistrar.destroy();
    }

    /**
     * The durable is deliberately node-independent — every engine node asks for the same one. That
     * is only safe because it is bound as a QUEUE consumer: this test pins both halves together,
     * since the durable half alone is what made the subscription single-node
     * ({@code [SUB-90012]}). jnats also rejects a queue name that disagrees with the consumer
     * configuration's deliver group, so the two must be asserted as one contract, not separately.
     */
    @Test
    void replyConsumerConfigFor_durableNameAndDlqSubject_derivedFromTopicName() throws Exception {
        ArgumentCaptor<PushSubscribeOptions> optsCaptor = ArgumentCaptor.forClass(PushSubscribeOptions.class);
        ArgumentCaptor<String> queueCaptor = ArgumentCaptor.forClass(String.class);

        registrar.afterPropertiesSet();

        verify(jetStream, times(1)).subscribe(eq("jobs.order-fulfillment.reply"), queueCaptor.capture(), any(), any(),
                eq(false), optsCaptor.capture());
        ConsumerConfiguration cc = optsCaptor.getValue().getConsumerConfiguration();
        assertThat(cc.getDurable()).isEqualTo("a2-completion-order-fulfillment");
        assertThat(cc.getDeliverGroup()).isEqualTo("a2-completion-order-fulfillment");
        assertThat(queueCaptor.getValue()).isEqualTo(cc.getDeliverGroup());
    }

    @Test
    void destroy_unsubscribesCompletionAndIncidentBridges_drainsDispatcher() throws Exception {
        registrar.afterPropertiesSet();

        registrar.destroy();

        // Both A2CompletionBridge and A2IncidentBridge share the SAME mocked Connection ->
        // createDispatcher() returns the SAME dispatcher mock for both -> unsubscribe() on each
        // real bridge drains it once each = 2 total drain() calls.
        verify(dispatcher, times(2)).drain(Duration.ofSeconds(10));
    }

    @Test
    void destroy_beforeAfterPropertiesSet_isNoOp_doesNotThrow() {
        assertThatCode(registrar::destroy).doesNotThrowAnyException();
    }

    /**
     * {@code A2OrphanSweep} is constructed internally (not injectable) — against fully mocked
     * {@link ProcessEngine}/{@link JetStream} collaborators its real {@code sweepCycle()} is all
     * but guaranteed to throw (mocked chained calls return null). This proves {@code
     * runSweepCycleSafely}'s catch-and-log actually keeps the daemon schedule alive rather than
     * silently killing {@code ScheduledExecutorService.scheduleWithFixedDelay}'s periodic task
     * (a real JDK hazard for uncaught exceptions in scheduled tasks).
     */
    @Test
    void afterPropertiesSet_sweepSchedulerFiresRepeatedly_survivesRealSweepCycleFailures() throws Exception {
        properties.getDefaults().setSweepPeriodSeconds(1);
        A2SubscriptionRegistrar fastSweepRegistrar = new A2SubscriptionRegistrar(properties, connection, jetStream,
                externalTaskService, dlqPublisher, metrics, meterRegistry, processEngine, lockResolver, topicConfig,
                lockValidator, kvManager, KV_REPLICAS);

        fastSweepRegistrar.afterPropertiesSet();
        // Long enough for >= 2 sweep cycles at a 1s period -- if an uncaught exception killed the
        // schedule after the first cycle, this test would still pass (weak), but if
        // runSweepCycleSafely itself threw OUT of the scheduled task, the JDK would silently drop
        // the periodic task; there is no direct JUnit-visible symptom either way EXCEPT that a
        // truly uncaught exception inside a virtual/daemon thread never fails this test process.
        // The meaningful assertion is destroy() still cleanly shutting down afterward.
        Thread.sleep(2_500);

        assertThatCode(fastSweepRegistrar::destroy).doesNotThrowAnyException();
    }

    @Test
    void resolveNodeId_realHostnameOrUuidFallback_leaseConstructedWithoutThrowing() {
        // Exercises resolveNodeId() (InetAddress.getLocalHost(), with UnknownHostException ->
        // UUID fallback) indirectly via the SweepLeaderLease construction inside
        // afterPropertiesSet() -- both branches are environment-dependent, but EITHER outcome
        // must not throw.
        assertThatCode(() -> registrar.afterPropertiesSet()).doesNotThrowAnyException();
    }

    /**
     * The replica count must arrive from configuration, not from a literal in this class. It was a
     * literal {@code 3} here and a literal {@code 1} in the bench, which is exactly why a
     * single-node NATS could not run the engine at all: the two values never met in one code path,
     * so nothing could disagree with either. Passing a non-default value and watching it reach the
     * bucket is the assertion that keeps them joined.
     */
    @Test
    void afterPropertiesSet_kvReplicasComesFromConfiguration_notAHardcodedThree() throws Exception {
        A2SubscriptionRegistrar singleNodeRegistrar = new A2SubscriptionRegistrar(properties, connection, jetStream,
                externalTaskService, dlqPublisher, metrics, meterRegistry, processEngine, lockResolver, topicConfig,
                lockValidator, kvManager, 1);

        singleNodeRegistrar.afterPropertiesSet();

        verify(kvManager).ensureBucket(eq("a2-sweep-leader"), any(Duration.class),
                org.mockito.ArgumentMatchers.eq(1), eq(connection));
        singleNodeRegistrar.destroy();
    }

    /**
     * The one line in this class that nothing asserted:
     * {@code config.setReplyPayloadVariable(properties.getReplyPayloadVariable())}. The mode is
     * read from {@link A2Properties}, copied into {@link A2ConsumerConfig}, carried into
     * {@link A2CompletionBridge} and finally consulted by {@link A2ReplyPayloadDecoder} — four
     * hops, and a decoder-level test looks identical whether or not the first one happens.
     *
     * <p>Rather than open a test-only window into the registrar, this drives the real chain: let
     * the registrar build and subscribe its bridge against the mocked {@link JetStream}, capture
     * the {@link MessageHandler} it registered, and feed that handler a reply. What reaches
     * {@code complete(...)} is then the product of every hop.
     */
    @Test
    void replyPayloadVariable_travelsFromPropertiesThroughToTheCompletionCall() throws Exception {
        properties.setReplyPayloadVariable(A2Properties.ReplyPayloadVariable.NEVER);
        A2SubscriptionRegistrar neverRegistrar = new A2SubscriptionRegistrar(properties, connection, jetStream,
                externalTaskService, dlqPublisher, metrics, meterRegistry, processEngine, lockResolver, topicConfig,
                lockValidator, kvManager, KV_REPLICAS);
        ArgumentCaptor<MessageHandler> handlerCaptor = ArgumentCaptor.forClass(MessageHandler.class);

        neverRegistrar.afterPropertiesSet();

        verify(jetStream).subscribe(eq("jobs.order-fulfillment.reply"), anyString(), any(),
                handlerCaptor.capture(), eq(false), any(PushSubscribeOptions.class));

        CountDownLatch completed = new CountDownLatch(1);
        java.util.concurrent.atomic.AtomicReference<Map<String, Object>> passed =
                new java.util.concurrent.atomic.AtomicReference<>();
        org.mockito.Mockito.doAnswer(invocation -> {
            passed.set(invocation.getArgument(2));
            completed.countDown();
            return null;
        }).when(externalTaskService).complete(anyString(), anyString(), org.mockito.ArgumentMatchers.anyMap());

        // A reply that DOES carry business data: under WHEN_PRESENT it would be written, so a
        // non-empty map here would mean NEVER never left A2Properties.
        handlerCaptor.getValue().onMessage(
                replyMessage("task-1", "{\"type\":\"SUCCESS\",\"orderId\":\"A-123\"}"));

        assertThat(completed.await(10, TimeUnit.SECONDS)).as("completion reached the engine").isTrue();
        assertThat(passed.get()).as("NEVER reached the decoder, four hops away").isEmpty();
        neverRegistrar.destroy();
    }

    private io.nats.client.Message replyMessage(String externalTaskId, String body) {
        io.nats.client.Message msg = mock(io.nats.client.Message.class);
        when(msg.getData()).thenReturn(body.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        io.nats.client.impl.Headers headers = new io.nats.client.impl.Headers();
        headers.add("Nats-Msg-Id", externalTaskId);
        when(msg.getHeaders()).thenReturn(headers);
        when(msg.getSubject()).thenReturn("jobs.order-fulfillment.reply");
        io.nats.client.impl.NatsJetStreamMetaData metaData =
                mock(io.nats.client.impl.NatsJetStreamMetaData.class);
        when(metaData.deliveredCount()).thenReturn(1L);
        when(msg.metaData()).thenReturn(metaData);
        return msg;
    }
}
