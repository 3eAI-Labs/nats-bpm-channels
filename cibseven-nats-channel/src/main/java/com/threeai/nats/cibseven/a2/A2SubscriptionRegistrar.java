package com.threeai.nats.cibseven.a2;

import static net.logstash.logback.argument.StructuredArguments.kv;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.threeai.nats.core.dlq.DlqPublisher;
import com.threeai.nats.core.jetstream.JetStreamKvManager;
import com.threeai.nats.core.jetstream.SweepLeaderLease;
import com.threeai.nats.core.metrics.NatsChannelMetrics;
import com.threeai.nats.core.resilience.DlqBridgeCircuitBreakerFactory;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.micrometer.core.instrument.MeterRegistry;
import io.nats.client.Connection;
import io.nats.client.JetStream;
import org.cibseven.bpm.engine.ExternalTaskService;
import org.cibseven.bpm.engine.ProcessEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;

/**
 * Bootstraps the A2 wiring: one {@link A2CompletionBridge} per configured A2 topic (each topic
 * has its own {@code jobs.<topic>.reply} subject), a single wildcard {@link A2IncidentBridge}
 * covering {@code dlq.jobs.>} (all A2 topics share one DLQ-bridge circuit breaker,
 * {@code cb-incident-bridge-cibseven}, ADR-0004), the {@link SweepLeaderLease}/
 * {@link A2OrphanSweep} pair, and the KV-bucket bootstrap (99_deployment.md §1/§2).
 *
 * <p><b>CODER-QUESTION:</b> whether the incident-bridge should be one wildcard subscription (as
 * implemented here) or one instance per topic is not explicitly pinned by the LLD class sketch;
 * a single instance keeps "one CB per bridge type" (LLD 03_classes/1 §4.3) literally true and
 * avoids redundant per-topic subscriptions to the same shared DLQ stream.
 */
public class A2SubscriptionRegistrar implements InitializingBean, DisposableBean {

    private static final Logger log = LoggerFactory.getLogger(A2SubscriptionRegistrar.class);
    private static final String A2_RESERVED_PREFIX = "jobs.";
    private static final String ENGINE_ID = "cibseven";

    private final A2Properties properties;
    private final Connection connection;
    private final JetStream jetStream;
    private final ExternalTaskService externalTaskService;
    private final DlqPublisher dlqPublisher;
    private final NatsChannelMetrics metrics;
    private final MeterRegistry meterRegistry;
    private final ProcessEngine processEngine;
    private final UmbrellaLockResolver lockResolver;
    private final A2TopicConfig topicConfig;
    private final UmbrellaLockValidator lockValidator;
    private final JetStreamKvManager kvManager;

    private final List<A2CompletionBridge> completionBridges = new ArrayList<>();
    private A2IncidentBridge incidentBridge;
    private SweepLeaderLease sweepLeaderLease;
    private A2OrphanSweep orphanSweep;
    private ScheduledExecutorService sweepScheduler;

    public A2SubscriptionRegistrar(A2Properties properties, Connection connection, JetStream jetStream,
            ExternalTaskService externalTaskService, DlqPublisher dlqPublisher, NatsChannelMetrics metrics,
            MeterRegistry meterRegistry, ProcessEngine processEngine, UmbrellaLockResolver lockResolver,
            A2TopicConfig topicConfig, UmbrellaLockValidator lockValidator, JetStreamKvManager kvManager) {
        this.properties = properties;
        this.connection = connection;
        this.jetStream = jetStream;
        this.externalTaskService = externalTaskService;
        this.dlqPublisher = dlqPublisher;
        this.metrics = metrics;
        this.meterRegistry = meterRegistry;
        this.processEngine = processEngine;
        this.lockResolver = lockResolver;
        this.topicConfig = topicConfig;
        this.lockValidator = lockValidator;
        this.kvManager = kvManager;
    }

    @Override
    public void afterPropertiesSet() {
        if (properties.getTopics().isEmpty()) {
            return;
        }

        for (String topic : properties.getTopics()) {
            A2ConsumerConfig config = replyConsumerConfigFor(topic);
            A2CompletionBridge bridge = new A2CompletionBridge(
                    connection, jetStream, externalTaskService, properties.getSentinelWorkerId(),
                    config, dlqPublisher, metrics);
            bridge.subscribe();
            completionBridges.add(bridge);
            log.info("Registered A2 completion bridge", kv("topic", topic), kv("subject", config.getSubject()));
        }

        CircuitBreaker incidentCb = DlqBridgeCircuitBreakerFactory.create(
                "cb-incident-bridge-cibseven", meterRegistry,
                org.cibseven.bpm.engine.exception.NotFoundException.class);
        A2ConsumerConfig incidentConfig = new A2ConsumerConfig();
        incidentConfig.setSubject("dlq." + A2_RESERVED_PREFIX + ">");
        incidentConfig.setMessageName("a2-incident-bridge");
        incidentConfig.setAckWaitSeconds(properties.getDefaults().getAckWaitSeconds());
        incidentConfig.setMaxDeliver(properties.getDefaults().getMaxDeliver());
        incidentBridge = new A2IncidentBridge(connection, jetStream, externalTaskService,
                properties.getSentinelWorkerId(), incidentConfig, incidentCb, metrics);
        incidentBridge.subscribe();
        log.info("Registered A2 incident bridge (wildcard dlq.jobs.>)");

        kvManager.ensureBucket("a2-sweep-leader", java.time.Duration.ofSeconds(
                2 * properties.getDefaults().getSweepPeriodSeconds()), 3, connection);
        sweepLeaderLease = new SweepLeaderLease(jetStream, kvManager, connection, ENGINE_ID, resolveNodeId(),
                java.time.Duration.ofSeconds(2 * properties.getDefaults().getSweepPeriodSeconds()));
        orphanSweep = new A2OrphanSweep(processEngine, sweepLeaderLease, jetStream, topicConfig,
                properties.getSentinelWorkerId(), lockResolver, metrics, lockValidator);

        long sweepPeriodSeconds = properties.getDefaults().getSweepPeriodSeconds();
        sweepScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "a2-orphan-sweep-cibseven");
            t.setDaemon(true);
            return t;
        });
        sweepScheduler.scheduleWithFixedDelay(this::runSweepCycleSafely,
                sweepPeriodSeconds, sweepPeriodSeconds, TimeUnit.SECONDS);
    }

    private void runSweepCycleSafely() {
        try {
            orphanSweep.sweepCycle();
        } catch (Exception e) {
            log.error("Uncaught exception in A2 orphan-sweep cycle — will retry next cycle", e);
        }
    }

    private A2ConsumerConfig replyConsumerConfigFor(String topic) {
        A2Properties.TopicLockOverride override = properties.getTopicOverrides().get(topic);
        long ackWait = override != null && override.getAckWaitSeconds() != null
                ? override.getAckWaitSeconds() : properties.getDefaults().getAckWaitSeconds();
        int maxDeliver = override != null && override.getMaxDeliver() != null
                ? override.getMaxDeliver() : properties.getDefaults().getMaxDeliver();
        long retryTimeoutMillis = override != null && override.getRetryTimeoutMillis() != null
                ? override.getRetryTimeoutMillis() : properties.getDefaults().getRetryTimeoutMillis();

        A2ConsumerConfig config = new A2ConsumerConfig();
        config.setSubject(A2_RESERVED_PREFIX + topic + ".reply");
        config.setMessageName(topic);
        config.setDurableName("a2-completion-" + topic);
        config.setAckWaitSeconds(ackWait);
        config.setMaxDeliver(maxDeliver);
        config.setDlqSubject("dlq." + A2_RESERVED_PREFIX + topic);
        config.setRetryTimeoutMillis(retryTimeoutMillis);
        return config;
    }

    private String resolveNodeId() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            return "node-" + UUID.randomUUID();
        }
    }

    @Override
    public void destroy() {
        if (sweepScheduler != null) {
            sweepScheduler.shutdownNow();
        }
        completionBridges.forEach(A2CompletionBridge::unsubscribe);
        if (incidentBridge != null) {
            incidentBridge.unsubscribe();
        }
        log.info("All A2 subscriptions unsubscribed");
    }
}
