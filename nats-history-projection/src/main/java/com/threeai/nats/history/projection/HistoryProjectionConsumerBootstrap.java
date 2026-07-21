package com.threeai.nats.history.projection;

import static net.logstash.logback.argument.StructuredArguments.kv;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import com.threeai.nats.core.jetstream.JetStreamSubjectPartitioner;
import com.threeai.nats.core.metrics.NatsChannelMetrics;
import io.nats.client.Dispatcher;
import io.nats.client.JetStream;
import io.nats.client.PushSubscribeOptions;
import io.nats.client.api.ConsumerConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;

/**
 * Registers one {@link HistoryProjectionConsumer} per owned partition (BR-REL-002, ADR-0011,
 * `03_classes/2_relay_projection.md` §2). Partition ownership: {@link
 * HistoryProjectionProperties#getPartitionAssignment()} if non-empty, else {@code replicaOrdinal %
 * partitionCount} derived from the K8s StatefulSet pod ordinal (`01_overview.md` "#2").
 *
 * <p><b>CODER-NOTE:</b> the LLD describes this as a Spring {@code @Configuration} that "registers
 * one {@code HistoryProjectionConsumer} bean per partition" — since the partition COUNT is only
 * known at runtime (config-driven, not a compile-time bean list), this is implemented as an
 * {@link InitializingBean}/{@link DisposableBean} holding the resulting consumers/subscriptions
 * in a list, mirroring the already-established {@code A2SubscriptionRegistrar} pattern
 * (`camunda-nats-channel`) for the same "variable-cardinality NATS subscription" shape.
 *
 * <p><b>CODER-NOTE:</b> {@code HISTORY} stream creation (with its partition {@code
 * SubjectTransform}) is NOT performed here — {@code 01_overview.md} "#2" explicitly marks
 * partition-count changes as a maintenance-window runbook (`99_deployment.md §3`), not a
 * live/automatic operation; this bootstrap only creates the durable, partition-scoped
 * CONSUMER against a stream ops is assumed to have already provisioned. (Test/bench-environment
 * stream provisioning is `nats-bpm-bench`'s {@code HistoryStreamProvisioner}, module 5.)
 */
public class HistoryProjectionConsumerBootstrap implements InitializingBean, DisposableBean {

    private static final Logger log = LoggerFactory.getLogger(HistoryProjectionConsumerBootstrap.class);
    private static final String BASE_SUBJECT = "history";
    private static final int WILDCARD_COUNT = 3; // <engineId>.<class>.<processInstanceId>
    private static final Duration ACK_WAIT = Duration.ofSeconds(30); // asyncapi x-jetstream.ackWaitSeconds, fixed
    private static final int MAX_DELIVER = 4; // asyncapi x-jetstream.maxDeliver, fixed
    private static final String DURABLE_PREFIX = "history-projection-consumer-part-";

    private final JetStream jetStream;
    private final io.nats.client.Connection connection;
    private final ProjectionStore projectionStore;
    private final HistoryDlqConsumer dlqConsumer;
    private final NatsChannelMetrics metrics;
    private final HistoryProjectionProperties properties;

    private final List<HistoryProjectionConsumer> consumers = new ArrayList<>();
    private final List<Dispatcher> dispatchers = new ArrayList<>();

    public HistoryProjectionConsumerBootstrap(JetStream jetStream, io.nats.client.Connection connection,
            ProjectionStore projectionStore, HistoryDlqConsumer dlqConsumer,
            NatsChannelMetrics metrics, HistoryProjectionProperties properties) {
        this.jetStream = jetStream;
        this.connection = connection;
        this.projectionStore = projectionStore;
        this.dlqConsumer = dlqConsumer;
        this.metrics = metrics;
        this.properties = properties;
    }

    @Override
    public void afterPropertiesSet() {
        for (int partitionIndex : ownedPartitions()) {
            registerPartitionConsumer(partitionIndex);
        }
    }

    private List<Integer> ownedPartitions() {
        if (!properties.getPartitionAssignment().isEmpty()) {
            return properties.getPartitionAssignment();
        }
        int ordinal = resolveReplicaOrdinal();
        int owned = JetStreamSubjectPartitioner.resolvePartitionIndex(ordinal, properties.getPartitionCount());
        return List.of(owned);
    }

    private void registerPartitionConsumer(int partitionIndex) {
        HistoryProjectionConsumer consumer = new HistoryProjectionConsumer(partitionIndex, jetStream,
                projectionStore, dlqConsumer, metrics, MAX_DELIVER);
        String filterSubject = JetStreamSubjectPartitioner.partitionFilterSubject(BASE_SUBJECT, WILDCARD_COUNT, partitionIndex);
        String durableName = DURABLE_PREFIX + partitionIndex;
        try {
            Dispatcher dispatcher = connection.createDispatcher();
            ConsumerConfiguration cc = ConsumerConfiguration.builder()
                    .durable(durableName)
                    .filterSubject(filterSubject)
                    .ackWait(ACK_WAIT)
                    .maxDeliver(MAX_DELIVER + 1)
                    .build();
            PushSubscribeOptions opts = PushSubscribeOptions.builder().configuration(cc).build();
            jetStream.subscribe(filterSubject, dispatcher, consumer::onMessage, false, opts);
            consumers.add(consumer);
            dispatchers.add(dispatcher);
            log.info("Registered history projection consumer", kv("partition_index", partitionIndex),
                    kv("filter_subject", filterSubject), kv("durable_name", durableName));
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Failed to subscribe history projection consumer for partition " + partitionIndex, e);
        }
    }

    private int resolveReplicaOrdinal() {
        String hostname;
        try {
            hostname = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            return 0;
        }
        int lastDash = hostname.lastIndexOf('-');
        if (lastDash < 0 || lastDash == hostname.length() - 1) {
            return 0;
        }
        try {
            return Integer.parseInt(hostname.substring(lastDash + 1));
        } catch (NumberFormatException notOrdinalSuffix) {
            return 0;
        }
    }

    @Override
    public void destroy() {
        for (Dispatcher dispatcher : dispatchers) {
            try {
                dispatcher.drain(Duration.ofSeconds(10));
            } catch (Exception e) {
                log.warn("Error draining history projection consumer dispatcher", e);
            }
        }
        consumers.clear();
    }
}
