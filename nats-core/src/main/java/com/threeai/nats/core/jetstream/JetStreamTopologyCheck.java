package com.threeai.nats.core.jetstream;

import static net.logstash.logback.argument.StructuredArguments.kv;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import io.nats.client.Connection;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamManagement;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.ConsumerInfo;
import io.nats.client.api.RetentionPolicy;
import io.nats.client.api.StreamInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Startup-time comparison between what the broker actually holds and what running multi-node
 * requires. Reports; never repairs, and never fails a boot.
 *
 * <p>Every fault this looks for is one this library's deployments hit for real — most were
 * invisible until a second engine node existed:
 *
 * <ul>
 *   <li>a durable push consumer with no deliver group is <em>exclusive</em> — the first node binds
 *       it and every other node dies with {@code [SUB-90012]}. JetStream will not add a deliver
 *       group to a consumer that already exists ({@code [SUB-90016]}), so an upgraded deployment
 *       carrying 0.8.0's consumers is broken until they are deleted by hand. The library creates
 *       them correctly now; what it cannot do is fix the ones already on the server;</li>
 *   <li>a single-replica stream on a clustered server works perfectly until the node holding it is
 *       lost, then stops serving entirely. {@link JetStreamStreamManager} warns when it creates
 *       one, but a stream provisioned by an earlier version or by an operator never passes through
 *       that path;</li>
 *   <li>KV replicas above 1 on a non-clustered server are rejected outright
 *       ({@code replicas > 1 not supported in non-clustered mode [10074]}) while the engine is
 *       starting — a configuration mistake that surfaces as a failed boot rather than as a
 *       configuration message;</li>
 *   <li>a limits-retention stream with no size cap grows with throughput — acknowledgement does
 *       not remove messages, only limits do, so consumed messages live on for the full max-age.
 *       The 2026-08-20 sustained-load exercise OOMed a 512&nbsp;MiB broker node exactly this way,
 *       twice, with the work/reply streams at 587+463&nbsp;MiB inside a one-hour max-age.</li>
 * </ul>
 *
 * <h2>Why this reports instead of fixing</h2>
 *
 * <p>Deleting a durable consumer resets its delivery position, so replies still retained in the
 * stream are redelivered. That is an acceptable cost when an operator chooses it during an upgrade
 * window, and an unacceptable one for a library to choose on their behalf during a rolling restart.
 * The finding names the consumer and the command; the decision stays with the operator.
 *
 * <h2>Why this cannot fail a boot</h2>
 *
 * <p>A diagnostic that takes the context down is the same class of defect it exists to prevent —
 * 0.8.1 shipped two of those. Every broker error here is swallowed at debug level: if the check
 * cannot reach JetStream, the application starts anyway and the paths that genuinely need
 * JetStream fail on their own terms, with their own messages.
 */
public final class JetStreamTopologyCheck {

    private static final Logger log = LoggerFactory.getLogger(JetStreamTopologyCheck.class);

    private static final int NOT_FOUND = 404;

    private JetStreamTopologyCheck() {
    }

    /**
     * A durable consumer this deployment will bind, identified the way JetStream identifies it.
     *
     * @param stream      stream holding the consumer
     * @param durableName durable name, as passed to {@code ConsumerConfiguration.durable(...)}
     */
    public record ConsumerBinding(String stream, String durableName) {
    }

    /**
     * A durable consumer identified by the SUBJECT it subscribes to rather than the stream
     * holding it. The library never names streams — the operator provisions them — so callers
     * know their subjects ({@code jobs.<topic>.reply}, {@code dlq.jobs.>}) but not the streams
     * behind them. {@link #inspectAndWarnSubjects} resolves each subject to its owning stream on
     * the live broker before running the same checks.
     */
    public record SubjectBinding(String subject, String durableName) {
    }

    /**
     * One problem found on the live broker.
     *
     * @param code    stable identifier, safe to alert on
     * @param message what is wrong and what to do about it, in one sentence
     */
    public record Finding(String code, String message) {
    }

    /** A durable push consumer exists without a deliver group; only one node can bind it. */
    public static final String DURABLE_WITHOUT_DELIVER_GROUP = "DURABLE_WITHOUT_DELIVER_GROUP";

    /** A stream this deployment uses has one replica while the server reports a cluster. */
    public static final String SINGLE_REPLICA_STREAM_ON_CLUSTER = "SINGLE_REPLICA_STREAM_ON_CLUSTER";

    /** KV replicas are configured above 1 on a server that is not clustered. */
    public static final String KV_REPLICAS_WITHOUT_CLUSTER = "KV_REPLICAS_WITHOUT_CLUSTER";

    /**
     * A limits-retention stream this deployment uses has no byte or message cap; its footprint
     * grows with throughput until the node holding it dies.
     */
    public static final String LIMITS_STREAM_WITHOUT_SIZE_CAP = "LIMITS_STREAM_WITHOUT_SIZE_CAP";

    /**
     * Inspects the broker and returns what is wrong, in the order the checks ran. An empty list
     * means every check that could be evaluated passed — it does not mean every check ran, because
     * a consumer or stream that does not exist yet is not a finding.
     *
     * @param connection          live NATS connection; a null or non-JetStream connection yields
     *                            an empty list
     * @param bindings            durable consumers this deployment will bind
     * @param configuredKvReplicas value of {@code spring.nats.jetstream.kv-replicas}
     */
    public static List<Finding> inspect(Connection connection, Collection<ConsumerBinding> bindings,
            int configuredKvReplicas) {
        List<Finding> findings = new ArrayList<>();
        if (connection == null) {
            return findings;
        }

        boolean clustered = isClustered(connection);
        if (!clustered && configuredKvReplicas > 1) {
            findings.add(new Finding(KV_REPLICAS_WITHOUT_CLUSTER,
                    "spring.nats.jetstream.kv-replicas is " + configuredKvReplicas + " but the server is not "
                            + "clustered; bucket creation will be rejected with [10074]. Set it to 1 for a "
                            + "single-node server."));
        }

        JetStreamManagement jsm;
        try {
            jsm = connection.jetStreamManagement();
        } catch (IOException e) {
            log.debug("Topology check skipped — JetStream management unavailable", e);
            return findings;
        }
        if (jsm == null) {
            log.debug("Topology check skipped — no JetStream management context");
            return findings;
        }

        for (ConsumerBinding binding : bindings) {
            checkConsumer(jsm, binding).ifPresent(findings::add);
        }
        for (String stream : distinctStreams(bindings)) {
            StreamInfo info = fetchStream(jsm, stream);
            if (info == null) {
                continue;
            }
            if (clustered) {
                checkStreamReplicas(info, stream).ifPresent(findings::add);
            }
            checkStreamRetention(info, stream).ifPresent(findings::add);
        }
        return findings;
    }

    /**
     * {@link #inspect} plus a WARN line per finding, which is how callers are expected to use it.
     * Returns the findings so a caller that wants to do more than log still can.
     */
    public static List<Finding> inspectAndWarn(Connection connection, Collection<ConsumerBinding> bindings,
            int configuredKvReplicas) {
        List<Finding> findings = inspect(connection, bindings, configuredKvReplicas);
        for (Finding finding : findings) {
            log.warn("NATS topology check: {}", finding.message(), kv("code", finding.code()));
        }
        return findings;
    }

    /**
     * {@link #inspectAndWarn} for callers that know subjects rather than stream names. Subjects
     * are resolved against the live broker; a subject with no stream behind it yet (first boot,
     * auto-created streams) is skipped — not provisioned is not a finding, and the next start
     * picks it up. Resolution failures are debug-logged and skipped; the KV check runs
     * regardless. Never fails a boot.
     */
    public static List<Finding> inspectAndWarnSubjects(Connection connection,
            Collection<SubjectBinding> subjectBindings, int configuredKvReplicas) {
        return inspectAndWarn(connection, resolveBindings(connection, subjectBindings), configuredKvReplicas);
    }

    static List<ConsumerBinding> resolveBindings(Connection connection,
            Collection<SubjectBinding> subjectBindings) {
        List<ConsumerBinding> resolved = new ArrayList<>();
        if (connection == null || subjectBindings == null || subjectBindings.isEmpty()) {
            return resolved;
        }
        JetStreamManagement jsm;
        try {
            jsm = connection.jetStreamManagement();
        } catch (IOException e) {
            log.debug("Topology check subject resolution skipped — JetStream management unavailable", e);
            return resolved;
        }
        if (jsm == null) {
            // A mocked or non-JetStream connection hands back null instead of throwing — treat it
            // exactly like an unreachable management context: skip, never fail a boot.
            log.debug("Topology check subject resolution skipped — no JetStream management context");
            return resolved;
        }
        for (SubjectBinding binding : subjectBindings) {
            try {
                for (String stream : jsm.getStreamNames(binding.subject())) {
                    resolved.add(new ConsumerBinding(stream, binding.durableName()));
                }
            } catch (IOException | JetStreamApiException e) {
                log.debug("Topology check skipped for subject {}", binding.subject(), e);
            }
        }
        return resolved;
    }

    /**
     * Keyed off {@code ServerInfo.getCluster()} — the server's own statement about how it runs —
     * for the same reason {@link JetStreamStreamManager} uses it: a stream's own cluster
     * information looks identical for a single-replica stream whether or not a cluster surrounds
     * it.
     */
    private static boolean isClustered(Connection connection) {
        var serverInfo = connection.getServerInfo();
        if (serverInfo == null) {
            return false;
        }
        String cluster = serverInfo.getCluster();
        return cluster != null && !cluster.isBlank();
    }

    private static Optional<Finding> checkConsumer(JetStreamManagement jsm, ConsumerBinding binding) {
        ConsumerInfo info;
        try {
            info = jsm.getConsumerInfo(binding.stream(), binding.durableName());
        } catch (JetStreamApiException e) {
            if (e.getErrorCode() != NOT_FOUND) {
                log.debug("Topology check skipped for consumer {}/{}", binding.stream(), binding.durableName(), e);
            }
            return Optional.empty();
        } catch (IOException e) {
            log.debug("Topology check skipped for consumer {}/{}", binding.stream(), binding.durableName(), e);
            return Optional.empty();
        }

        ConsumerConfiguration config = info.getConsumerConfiguration();
        boolean push = config.getDeliverSubject() != null && !config.getDeliverSubject().isBlank();
        String deliverGroup = config.getDeliverGroup();
        if (push && (deliverGroup == null || deliverGroup.isBlank())) {
            return Optional.of(new Finding(DURABLE_WITHOUT_DELIVER_GROUP,
                    "Durable consumer '" + binding.durableName() + "' on stream '" + binding.stream()
                            + "' has no deliver group, so only one node can bind it and the others fail with "
                            + "[SUB-90012]. JetStream cannot add a deliver group to an existing consumer "
                            + "([SUB-90016]) — delete it during an upgrade window: nats consumer rm "
                            + binding.stream() + " " + binding.durableName() + ". Deleting resets the delivery "
                            + "position, so retained messages are redelivered."));
        }
        return Optional.empty();
    }

    private static StreamInfo fetchStream(JetStreamManagement jsm, String stream) {
        try {
            return jsm.getStreamInfo(stream);
        } catch (JetStreamApiException e) {
            if (e.getErrorCode() != NOT_FOUND) {
                log.debug("Topology check skipped for stream {}", stream, e);
            }
            return null;
        } catch (IOException e) {
            log.debug("Topology check skipped for stream {}", stream, e);
            return null;
        }
    }

    private static Optional<Finding> checkStreamReplicas(StreamInfo info, String stream) {
        if (info.getConfiguration().getReplicas() <= 1) {
            return Optional.of(new Finding(SINGLE_REPLICA_STREAM_ON_CLUSTER,
                    "Stream '" + stream + "' has one replica on a clustered server; it stops serving entirely "
                            + "if the node holding it is lost. Existing streams are never reconfigured by this "
                            + "library — change it with nats stream edit " + stream + " --replicas 3, or set "
                            + "spring.nats.jetstream.stream-replicas before the stream is first created."));
        }
        return Optional.empty();
    }

    /**
     * Applies on clustered AND single-node servers alike — an uncapped stream OOMs a small broker
     * either way, and unlike the replica question, capping is actionable everywhere. A null
     * retention policy (only mocks produce one) is treated as not-limits.
     */
    private static Optional<Finding> checkStreamRetention(StreamInfo info, String stream) {
        var config = info.getConfiguration();
        boolean uncapped = config.getMaxBytes() <= 0 && config.getMaxMsgs() <= 0
                && config.getMaxMsgsPerSubject() <= 0;
        if (config.getRetentionPolicy() == RetentionPolicy.Limits && uncapped) {
            return Optional.of(new Finding(LIMITS_STREAM_WITHOUT_SIZE_CAP,
                    "Stream '" + stream + "' keeps messages on limits retention with no byte or message cap: "
                            + "acknowledgement does not remove them, so the footprint is bounded only by "
                            + "max-age times throughput — a work stream shaped exactly like this OOMed a "
                            + "512 MiB broker node in sustained-load testing. Cap it: nats stream edit "
                            + stream + " --max-bytes=<budget>; a work stream whose every consumer acks can "
                            + "use interest retention instead, while DLQ-style streams should keep limits "
                            + "retention and take the cap."));
        }
        return Optional.empty();
    }

    private static Set<String> distinctStreams(Collection<ConsumerBinding> bindings) {
        Set<String> streams = new LinkedHashSet<>();
        for (ConsumerBinding binding : bindings) {
            streams.add(binding.stream());
        }
        return streams;
    }
}
