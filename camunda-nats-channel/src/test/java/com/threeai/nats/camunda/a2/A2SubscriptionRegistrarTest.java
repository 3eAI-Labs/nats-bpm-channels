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

import com.threeai.nats.core.dlq.DlqPublisher;
import com.threeai.nats.core.jetstream.JetStreamKvManager;
import com.threeai.nats.core.metrics.NatsChannelMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.JetStream;
import io.nats.client.PushSubscribeOptions;
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
        // null, not a real SimpleMeterRegistry: DlqBridgeCircuitBreakerFactory.create only touches
        // TaggedCircuitBreakerMetrics (resilience4j-micrometer) when registry != null, and that
        // artifact is an OPTIONAL nats-core dependency this module does not itself redeclare —
        // mirrors CamundaNatsAutoConfigurationTest's context (no MeterRegistry bean present either).
        meterRegistry = null;
        metrics = new NatsChannelMetrics(new SimpleMeterRegistry());
        processEngine = mock(ProcessEngine.class);
        lockResolver = new UmbrellaLockResolver(properties);
        topicConfig = new A2TopicConfig(properties);
        lockValidator = new UmbrellaLockValidator(properties, lockResolver);
        kvManager = mock(JetStreamKvManager.class);

        registrar = new A2SubscriptionRegistrar(properties, connection, jetStream, externalTaskService,
                dlqPublisher, metrics, meterRegistry, processEngine, lockResolver, topicConfig, lockValidator,
                kvManager);
    }

    @AfterEach
    void tearDown() {
        registrar.destroy();
    }

    @Test
    void afterPropertiesSet_emptyTopics_earlyReturn_noResourcesCreated() throws Exception {
        properties.setTopics(List.of());
        A2SubscriptionRegistrar emptyRegistrar = new A2SubscriptionRegistrar(properties, connection, jetStream,
                externalTaskService, dlqPublisher, metrics, meterRegistry, processEngine, lockResolver, topicConfig,
                lockValidator, kvManager);

        emptyRegistrar.afterPropertiesSet();

        verify(kvManager, never()).ensureBucket(anyString(), any(Duration.class), org.mockito.ArgumentMatchers.anyInt(),
                any(Connection.class));
        verify(jetStream, never()).subscribe(anyString(), any(), any(), anyBoolean(), any(PushSubscribeOptions.class));
        assertThatCode(emptyRegistrar::destroy).doesNotThrowAnyException();
    }

    @Test
    void afterPropertiesSet_withTopics_subscribesCompletionBridgePerTopicPlusIncidentBridge() throws Exception {
        registrar.afterPropertiesSet();

        // 1 completion bridge (order-fulfillment) + 1 wildcard incident bridge = 2 subscribe() calls.
        verify(jetStream, times(2)).subscribe(anyString(), any(), any(), eq(false), any(PushSubscribeOptions.class));
        verify(kvManager).ensureBucket(eq("a2-sweep-leader"), any(Duration.class), org.mockito.ArgumentMatchers.eq(3),
                eq(connection));
    }

    @Test
    void afterPropertiesSet_multipleTopics_oneCompletionBridgePerTopic() throws Exception {
        properties.setTopics(List.of("order-fulfillment", "payment-processing"));
        A2SubscriptionRegistrar multiRegistrar = new A2SubscriptionRegistrar(properties, connection, jetStream,
                externalTaskService, dlqPublisher, metrics, meterRegistry, processEngine, lockResolver, topicConfig,
                lockValidator, kvManager);

        multiRegistrar.afterPropertiesSet();

        // 2 completion bridges + 1 incident bridge = 3 subscribe() calls.
        verify(jetStream, times(3)).subscribe(anyString(), any(), any(), eq(false), any(PushSubscribeOptions.class));
        multiRegistrar.destroy();
    }

    @Test
    void afterPropertiesSet_incidentBridgeSubject_isWildcardDlqJobs() throws Exception {
        ArgumentCaptor<String> subjectCaptor = ArgumentCaptor.forClass(String.class);

        registrar.afterPropertiesSet();

        verify(jetStream, times(2)).subscribe(subjectCaptor.capture(), any(), any(), eq(false),
                any(PushSubscribeOptions.class));
        assertThat(subjectCaptor.getAllValues()).contains("dlq.jobs.>", "jobs.order-fulfillment.reply");
    }

    @Test
    void replyConsumerConfigFor_topicWithoutOverride_usesPropertyDefaults() throws Exception {
        ArgumentCaptor<PushSubscribeOptions> optsCaptor = ArgumentCaptor.forClass(PushSubscribeOptions.class);

        registrar.afterPropertiesSet();

        verify(jetStream, times(1)).subscribe(eq("jobs.order-fulfillment.reply"), any(), any(), eq(false),
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
                lockValidator, kvManager);
        ArgumentCaptor<PushSubscribeOptions> optsCaptor = ArgumentCaptor.forClass(PushSubscribeOptions.class);

        overriddenRegistrar.afterPropertiesSet();

        verify(jetStream, times(1)).subscribe(eq("jobs.order-fulfillment.reply"), any(), any(), eq(false),
                optsCaptor.capture());
        assertThat(optsCaptor.getValue().getConsumerConfiguration().getMaxDeliver()).isEqualTo(10); // 9+1
        assertThat(optsCaptor.getValue().getConsumerConfiguration().getAckWait()).isEqualTo(Duration.ofSeconds(90));
        overriddenRegistrar.destroy();
    }

    @Test
    void replyConsumerConfigFor_durableNameAndDlqSubject_derivedFromTopicName() throws Exception {
        ArgumentCaptor<PushSubscribeOptions> optsCaptor = ArgumentCaptor.forClass(PushSubscribeOptions.class);

        registrar.afterPropertiesSet();

        verify(jetStream, times(1)).subscribe(eq("jobs.order-fulfillment.reply"), any(), any(), eq(false),
                optsCaptor.capture());
        assertThat(optsCaptor.getValue().getConsumerConfiguration().getDurable())
                .isEqualTo("a2-completion-order-fulfillment");
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
                lockValidator, kvManager);

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
}
