package com.threeai.nats.flowable.externalworker;

import static net.logstash.logback.argument.StructuredArguments.kv;

import java.time.Duration;
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
import io.nats.client.Connection;
import io.nats.client.JetStream;
import org.flowable.engine.ProcessEngine;
import org.flowable.engine.impl.cfg.ProcessEngineConfigurationImpl;
import org.flowable.engine.interceptor.CreateExternalWorkerJobInterceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Lifecycle root of the external-worker dispatch capability (docs/11 D-F'/D-G'): wires the
 * born-locked interceptor into the engine configuration (delegate-wrapping any host-installed
 * interceptor, never silently overwriting it), ensures the sweep-leader KV bucket (the shared
 * {@code a2-sweep-leader} bucket — the {@code a2-} prefix is historical, ops note), starts one
 * completion bridge per topic, the wildcard incident consumer, and the sweep scheduler.
 *
 * <p>Dual-gated opt-in: the auto-configuration is behind the explicit {@code enabled} flag AND
 * this registrar is inert when the topic list is empty.
 */
public class EwSubscriptionRegistrar {

    private static final Logger log = LoggerFactory.getLogger(EwSubscriptionRegistrar.class);

    private final Connection connection;
    private final JetStream jetStream;
    private final JetStreamKvManager kvManager;
    private final ProcessEngine processEngine;
    private final EwProperties properties;
    private final EwTopicConfig topicConfig;
    private final EwLockConfig lockConfig;
    private final DlqPublisher dlqPublisher;
    private final NatsChannelMetrics metrics;
    private final int kvReplicas;

    private final List<EwCompletionBridge> bridges = new ArrayList<>();
    private EwIncidentConsumer incidentConsumer;
    private ScheduledExecutorService sweepScheduler;

    public EwSubscriptionRegistrar(Connection connection, JetStream jetStream,
            JetStreamKvManager kvManager, ProcessEngine processEngine, EwProperties properties,
            EwTopicConfig topicConfig, EwLockConfig lockConfig, DlqPublisher dlqPublisher,
            NatsChannelMetrics metrics, int kvReplicas) {
        this.connection = connection;
        this.jetStream = jetStream;
        this.kvManager = kvManager;
        this.processEngine = processEngine;
        this.properties = properties;
        this.topicConfig = topicConfig;
        this.lockConfig = lockConfig;
        this.dlqPublisher = dlqPublisher;
        this.metrics = metrics;
        this.kvReplicas = kvReplicas;
    }

    public void start() {
        if (!properties.isEnabled() || topicConfig.topics().isEmpty()) {
            return;
        }
        String sentinel = properties.getSentinelWorkerId();

        // Dispatch seam: delegate-wrap, never overwrite (N-yellow-10b).
        ProcessEngineConfigurationImpl engineConfiguration =
                (ProcessEngineConfigurationImpl) processEngine.getProcessEngineConfiguration();
        CreateExternalWorkerJobInterceptor existing =
                engineConfiguration.getCreateExternalWorkerJobInterceptor();
        EwPostCommitPublisher publisher = new EwPostCommitPublisher(jetStream, metrics, lockConfig,
                properties.isAsyncPublish()
                        ? new com.threeai.nats.core.jetstream.BoundedAsyncPublisher(
                                jetStream, properties.getAsyncPublishMaxInFlight())
                        : null); // kacis: spring.nats.flowable.external-worker.async-publish=false
        engineConfiguration.setCreateExternalWorkerJobInterceptor(
                new EwCreateJobInterceptor(topicConfig, lockConfig, sentinel, publisher, existing));
        if (existing != null) {
            log.info("Existing CreateExternalWorkerJobInterceptor wrapped as delegate",
                    kv("delegate", existing.getClass().getName()));
        }

        // Sweep: shared leader bucket, engine id "flowable" -> key sweep-leader.flowable.
        Duration leaseTtl = Duration.ofSeconds(2 * properties.getSweepPeriodSeconds());
        kvManager.ensureBucket("a2-sweep-leader", leaseTtl, kvReplicas, connection);
        SweepLeaderLease lease = new SweepLeaderLease(jetStream, kvManager, connection,
                "flowable", UUID.randomUUID().toString(), leaseTtl);
        EwOrphanSweep sweep = new EwOrphanSweep(processEngine.getManagementService(), topicConfig,
                lockConfig, sentinel, jetStream, lease, metrics, properties.getSweepBatchSize());
        sweepScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "flw-ew-sweep");
            t.setDaemon(true);
            return t;
        });
        sweepScheduler.scheduleWithFixedDelay(sweep::runCycleSafely,
                properties.getSweepPeriodSeconds(), properties.getSweepPeriodSeconds(), TimeUnit.SECONDS);

        for (String topic : topicConfig.topics()) {
            EwConsumerConfig consumerConfig = new EwConsumerConfig(topic);
            consumerConfig.setAckWaitSeconds(properties.getAckWaitSeconds());
            consumerConfig.setMaxDeliver(properties.getMaxDeliver());
            consumerConfig.setRetryTimeoutMillis(properties.getRetryTimeoutMillis());
            EwCompletionBridge bridge = new EwCompletionBridge(connection, jetStream,
                    processEngine.getManagementService(), consumerConfig, sentinel, dlqPublisher, metrics);
            bridge.subscribe();
            bridges.add(bridge);
        }

        incidentConsumer = new EwIncidentConsumer(connection, jetStream,
                processEngine.getManagementService(), sentinel, properties.getAckWaitSeconds(), metrics);
        incidentConsumer.subscribe();

        log.info("External-worker dispatch active",
                kv("topics", topicConfig.topics()), kv("sentinel", sentinel),
                kv("lock_duration_ms", lockConfig.lockDurationMillis()));
    }

    public void stop() {
        for (EwCompletionBridge bridge : bridges) {
            bridge.unsubscribe();
        }
        if (incidentConsumer != null) {
            incidentConsumer.unsubscribe();
        }
        if (sweepScheduler != null) {
            sweepScheduler.shutdownNow();
        }
    }
}
