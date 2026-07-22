package org.flowable.eventregistry.spring.nats;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import com.threeai.nats.core.config.NamespaceValidator;
import com.threeai.nats.core.dlq.DlqPublisher;
import com.threeai.nats.core.exception.TopicNamespaceCollisionException;
import com.threeai.nats.core.jetstream.JetStreamStreamManager;
import com.threeai.nats.core.metrics.NatsChannelMetrics;
import io.nats.client.Connection;
import io.nats.client.JetStream;
import org.flowable.common.engine.api.FlowableException;
import org.flowable.eventregistry.api.ChannelModelProcessor;
import org.flowable.eventregistry.api.EventRegistry;
import org.flowable.eventregistry.api.EventRepositoryService;
import org.flowable.eventregistry.model.ChannelModel;
import org.flowable.eventregistry.model.InboundChannelModel;
import org.flowable.eventregistry.spring.nats.channel.NatsInboundChannelModel;
import org.flowable.eventregistry.spring.nats.channel.NatsOutboundChannelModel;
import org.flowable.eventregistry.spring.nats.jetstream.JetStreamInboundEventChannelAdapter;
import org.flowable.eventregistry.spring.nats.jetstream.JetStreamOutboundEventChannelAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NatsChannelDefinitionProcessor implements ChannelModelProcessor {

    private static final Logger log = LoggerFactory.getLogger(NatsChannelDefinitionProcessor.class);

    private final Connection connection;
    private final JetStream jetStream;
    private final JetStreamStreamManager streamManager;
    private final NatsChannelMetrics metrics;
    private final DlqPublisher dlqPublisher;
    private final Map<String, NatsInboundEventChannelAdapter> coreInboundAdapters = new ConcurrentHashMap<>();
    private final Map<String, JetStreamInboundEventChannelAdapter> jetStreamInboundAdapters = new ConcurrentHashMap<>();
    /** subject -> model lookup for {@code FailureEventBridge} (LLD 03_classes/4_flowable.md §2.2). */
    private final Map<String, InboundChannelModel> subjectToModel = new ConcurrentHashMap<>();

    public NatsChannelDefinitionProcessor(Connection connection, JetStream jetStream,
            JetStreamStreamManager streamManager, NatsChannelMetrics metrics, DlqPublisher dlqPublisher) {
        this.connection = connection;
        this.jetStream = jetStream;
        this.streamManager = streamManager;
        this.metrics = metrics;
        this.dlqPublisher = dlqPublisher;
    }

    /**
     * Used by {@code FailureEventBridge} to reconstruct the original inbound channel model from
     * the {@code X-Cadenzaflow-Dlq-Original-Subject} header of a DLQ message (contract-fix #1
     * makes this header reliably present).
     */
    public Optional<InboundChannelModel> findBySubject(String subject) {
        return Optional.ofNullable(subjectToModel.get(subject));
    }

    @Override
    public boolean canProcess(ChannelModel channelModel) {
        return channelModel instanceof NatsInboundChannelModel
                || channelModel instanceof NatsOutboundChannelModel;
    }

    @Override
    public boolean canProcessIfChannelModelAlreadyRegistered(ChannelModel channelModel) {
        return channelModel instanceof NatsOutboundChannelModel;
    }

    @Override
    public void registerChannelModel(ChannelModel channelModel, String tenantId,
            EventRegistry eventRegistry, EventRepositoryService eventRepositoryService,
            boolean fallbackToDefaultTenant) {

        if (channelModel instanceof NatsInboundChannelModel inboundModel) {
            if (inboundModel.isJetstream()) {
                registerJetStreamInbound(inboundModel, tenantId, eventRegistry);
            } else {
                registerInbound(inboundModel, tenantId, eventRegistry);
            }
        } else if (channelModel instanceof NatsOutboundChannelModel outboundModel) {
            if (outboundModel.isJetstream()) {
                registerJetStreamOutbound(outboundModel);
            } else {
                registerOutbound(outboundModel);
            }
        }
    }

    @Override
    public void unregisterChannelModel(ChannelModel channelModel, String tenantId,
            EventRepositoryService eventRepositoryService) {

        String key = resolveKey(channelModel, tenantId);
        NatsInboundEventChannelAdapter coreAdapter = coreInboundAdapters.remove(key);
        if (coreAdapter != null) {
            coreAdapter.unsubscribe();
        }
        JetStreamInboundEventChannelAdapter jsAdapter = jetStreamInboundAdapters.remove(key);
        if (jsAdapter != null) {
            jsAdapter.unsubscribe();
        }
        if (channelModel instanceof InboundChannelModel inboundChannelModel) {
            subjectToModel.values().removeIf(m -> m == inboundChannelModel);
        }
    }

    private void registerInbound(NatsInboundChannelModel model, String tenantId,
            EventRegistry eventRegistry) {

        validateSubject(model.getSubject(), model.getKey());

        NatsInboundEventChannelAdapter adapter = new NatsInboundEventChannelAdapter(
                connection, model.getSubject(), model.getQueueGroup());

        model.setInboundEventChannelAdapter(adapter);
        adapter.setInboundChannelModel(model);
        adapter.setEventRegistry(eventRegistry);
        adapter.subscribe();

        coreInboundAdapters.put(resolveKey(model, tenantId), adapter);
    }

    private void registerJetStreamInbound(NatsInboundChannelModel model, String tenantId,
            EventRegistry eventRegistry) {

        validateSubject(model.getSubject(), model.getKey());

        if (model.isAutoCreateStream() && model.getStreamName() != null) {
            streamManager.ensureStream(model.getStreamName(), model.getSubject(), connection);
        }

        String dlqSubject = model.getDlqSubject();
        if (dlqSubject == null) {
            dlqSubject = "dlq." + model.getSubject();
        }

        JetStreamInboundEventChannelAdapter adapter = new JetStreamInboundEventChannelAdapter(
                connection, jetStream, model.getSubject(), model.getMaxDeliver(),
                dlqSubject, metrics, model.getKey(), dlqPublisher);

        model.setInboundEventChannelAdapter(adapter);
        adapter.setInboundChannelModel(model);
        adapter.setEventRegistry(eventRegistry);
        adapter.subscribe();

        jetStreamInboundAdapters.put(resolveKey(model, tenantId), adapter);
        subjectToModel.put(model.getSubject(), model);
    }

    private void registerOutbound(NatsOutboundChannelModel model) {
        validateSubject(model.getSubject(), model.getKey());

        String dlqSubject = resolveDlqSubject(model);
        NatsOutboundEventChannelAdapter adapter = new NatsOutboundEventChannelAdapter(
                connection, model.getSubject(), model.getKey(), dlqPublisher, dlqSubject);
        model.setOutboundEventChannelAdapter(adapter);
    }

    private void registerJetStreamOutbound(NatsOutboundChannelModel model) {
        validateSubject(model.getSubject(), model.getKey());

        if (model.isAutoCreateStream() && model.getStreamName() != null) {
            streamManager.ensureStream(model.getStreamName(), model.getSubject(), connection);
        }

        String dlqSubject = resolveDlqSubject(model);
        JetStreamOutboundEventChannelAdapter adapter = new JetStreamOutboundEventChannelAdapter(
                jetStream, model.getSubject(), metrics, model.getKey(), dlqPublisher, dlqSubject);
        model.setOutboundEventChannelAdapter(adapter);
    }

    /** D-G' — mirrors the inbound adapters' {@code "dlq." + subject} default when unconfigured. */
    private String resolveDlqSubject(NatsOutboundChannelModel model) {
        return model.getDlqSubject() != null ? model.getDlqSubject() : "dlq." + model.getSubject();
    }

    private void validateSubject(String subject, String channelKey) {
        if (subject == null || subject.isBlank()) {
            throw new FlowableException(
                    "NATS channel '" + channelKey + "': subject is required");
        }
        try {
            NamespaceValidator.assertNotReservedForA2(subject, channelKey);
            // D-E'/D-G' (docs/09-outbound-handoff.md) -- events.*/dlq.events.* are reserved for the
            // basamak-4 outbound-handoff mechanism; a tenant-defined Flowable channel must not
            // collide with either.
            NamespaceValidator.assertNotReservedForOutbound(subject, channelKey);
        } catch (TopicNamespaceCollisionException e) {
            // nats-core is engine-neutral and cannot depend on Flowable (see CODER-NOTES);
            // re-wrap so callers still see the FlowableException surface they expect.
            throw new FlowableException(e.getMessage(), e);
        }
    }

    private String resolveKey(ChannelModel channelModel, String tenantId) {
        if (tenantId != null) {
            return tenantId + "#" + channelModel.getKey();
        }
        return channelModel.getKey();
    }
}
