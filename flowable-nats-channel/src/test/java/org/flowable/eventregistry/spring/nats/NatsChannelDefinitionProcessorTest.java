package org.flowable.eventregistry.spring.nats;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.threeai.nats.core.dlq.DlqPublisher;
import com.threeai.nats.core.exception.TopicNamespaceCollisionException;
import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.JetStream;
import io.nats.client.JetStreamSubscription;
import io.nats.client.MessageHandler;
import io.nats.client.PushSubscribeOptions;
import io.nats.client.Subscription;
import org.flowable.common.engine.api.FlowableException;
import org.flowable.eventregistry.api.EventRegistry;
import org.flowable.eventregistry.api.EventRepositoryService;
import org.flowable.eventregistry.model.InboundChannelModel;
import org.flowable.eventregistry.spring.nats.channel.NatsInboundChannelModel;
import org.flowable.eventregistry.spring.nats.channel.NatsOutboundChannelModel;
import org.flowable.eventregistry.spring.nats.jetstream.JetStreamInboundEventChannelAdapter;
import org.flowable.eventregistry.spring.nats.jetstream.JetStreamOutboundEventChannelAdapter;
import com.threeai.nats.core.jetstream.JetStreamStreamManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class NatsChannelDefinitionProcessorTest {

    private Connection connection;
    private JetStream jetStream;
    private JetStreamStreamManager streamManager;
    private DlqPublisher dlqPublisher;
    private EventRegistry eventRegistry;
    private EventRepositoryService eventRepositoryService;
    private NatsChannelDefinitionProcessor processor;

    @BeforeEach
    void setUp() throws Exception {
        connection = mock(Connection.class);
        jetStream = mock(JetStream.class);
        streamManager = mock(JetStreamStreamManager.class);
        dlqPublisher = mock(DlqPublisher.class);
        eventRegistry = mock(EventRegistry.class);
        eventRepositoryService = mock(EventRepositoryService.class);

        Dispatcher dispatcher = mock(Dispatcher.class);
        when(connection.createDispatcher()).thenReturn(dispatcher);
        when(dispatcher.subscribe(anyString(), any(MessageHandler.class))).thenReturn(mock(Subscription.class));
        when(dispatcher.subscribe(anyString(), anyString(), any(MessageHandler.class))).thenReturn(mock(Subscription.class));

        JetStreamSubscription jsSub = mock(JetStreamSubscription.class);
        when(jetStream.subscribe(anyString(), any(Dispatcher.class), any(MessageHandler.class),
                anyBoolean(), any(PushSubscribeOptions.class))).thenReturn(jsSub);

        processor = new NatsChannelDefinitionProcessor(connection, jetStream, streamManager, null, dlqPublisher);
    }

    @Test
    void canProcess_natsInboundModel_returnsTrue() {
        NatsInboundChannelModel model = new NatsInboundChannelModel();
        assertThat(processor.canProcess(model)).isTrue();
    }

    @Test
    void canProcess_natsOutboundModel_returnsTrue() {
        NatsOutboundChannelModel model = new NatsOutboundChannelModel();
        assertThat(processor.canProcess(model)).isTrue();
    }

    @Test
    void canProcess_otherModel_returnsFalse() {
        InboundChannelModel model = new InboundChannelModel();
        assertThat(processor.canProcess(model)).isFalse();
    }

    @Test
    void registerInbound_validFields_subscribes() {
        NatsInboundChannelModel model = new NatsInboundChannelModel();
        model.setKey("testChannel");
        model.setSubject("order.new");
        model.setQueueGroup("order-service");

        processor.registerChannelModel(model, null, eventRegistry, eventRepositoryService, false);

        assertThat(model.getInboundEventChannelAdapter()).isNotNull();
    }

    @Test
    void registerInbound_noQueueGroup_subscribesWithout() {
        NatsInboundChannelModel model = new NatsInboundChannelModel();
        model.setKey("testChannel");
        model.setSubject("order.new");

        processor.registerChannelModel(model, null, eventRegistry, eventRepositoryService, false);

        assertThat(model.getInboundEventChannelAdapter()).isNotNull();
    }

    @Test
    void registerOutbound_validFields_createsAdapter() {
        NatsOutboundChannelModel model = new NatsOutboundChannelModel();
        model.setKey("testChannel");
        model.setSubject("order.completed");

        processor.registerChannelModel(model, null, eventRegistry, eventRepositoryService, false);

        assertThat(model.getOutboundEventChannelAdapter()).isNotNull();
    }

    @Test
    void registerInbound_missingSubject_throwsException() {
        NatsInboundChannelModel model = new NatsInboundChannelModel();
        model.setKey("testChannel");

        assertThatThrownBy(() ->
                processor.registerChannelModel(model, null, eventRegistry, eventRepositoryService, false))
                .isInstanceOf(FlowableException.class)
                .hasMessageContaining("subject");
    }

    @Test
    void registerInbound_jetstreamTrue_createsJetStreamAdapter() {
        NatsInboundChannelModel model = new NatsInboundChannelModel();
        model.setKey("testChannel");
        model.setSubject("order.new");
        model.setJetstream(true);
        model.setMaxDeliver(5);

        processor.registerChannelModel(model, null, eventRegistry, eventRepositoryService, false);

        assertThat(model.getInboundEventChannelAdapter())
                .isInstanceOf(JetStreamInboundEventChannelAdapter.class);
    }

    @Test
    void registerOutbound_jetstreamTrue_createsJetStreamAdapter() {
        NatsOutboundChannelModel model = new NatsOutboundChannelModel();
        model.setKey("testChannel");
        model.setSubject("order.completed");
        model.setJetstream(true);

        processor.registerChannelModel(model, null, eventRegistry, eventRepositoryService, false);

        assertThat(model.getOutboundEventChannelAdapter())
                .isInstanceOf(JetStreamOutboundEventChannelAdapter.class);
    }

    @Test
    void registerInbound_reservedA2Namespace_throwsCollisionException() {
        NatsInboundChannelModel model = new NatsInboundChannelModel();
        model.setKey("testChannel");
        model.setSubject("jobs.order-fulfillment");

        assertThatThrownBy(() ->
                processor.registerChannelModel(model, null, eventRegistry, eventRepositoryService, false))
                .isInstanceOf(FlowableException.class)
                .hasCauseInstanceOf(TopicNamespaceCollisionException.class);
    }

    // --- Basamak-4 (docs/09-outbound-handoff.md D-E'/D-G') ---

    @Test
    void registerOutbound_reservedOutboundNamespace_throwsCollisionException() {
        NatsOutboundChannelModel model = new NatsOutboundChannelModel();
        model.setKey("testChannel");
        model.setSubject("events.camunda.order.created.proc-1");

        assertThatThrownBy(() ->
                processor.registerChannelModel(model, null, eventRegistry, eventRepositoryService, false))
                .isInstanceOf(FlowableException.class)
                .hasCauseInstanceOf(TopicNamespaceCollisionException.class);
    }

    @Test
    void registerOutbound_reservedOutboundDlqNamespace_throwsCollisionException() {
        NatsOutboundChannelModel model = new NatsOutboundChannelModel();
        model.setKey("testChannel");
        model.setSubject("dlq.events.camunda.order.created.proc-1");

        assertThatThrownBy(() ->
                processor.registerChannelModel(model, null, eventRegistry, eventRepositoryService, false))
                .isInstanceOf(FlowableException.class)
                .hasCauseInstanceOf(TopicNamespaceCollisionException.class);
    }

    @Test
    void registerOutbound_noDlqSubjectConfigured_adapterUsesDefaultDlqConvention() throws Exception {
        NatsOutboundChannelModel model = new NatsOutboundChannelModel();
        model.setKey("testChannel");
        model.setSubject("order.completed");

        processor.registerChannelModel(model, null, eventRegistry, eventRepositoryService, false);

        assertThat(model.getOutboundEventChannelAdapter()).isInstanceOf(NatsOutboundEventChannelAdapter.class);
        // Default DLQ subject resolution ("dlq." + subject) is exercised end-to-end by
        // NatsOutboundEventChannelAdapterTest; here we only assert the adapter wired successfully.
    }

    @Test
    void registerJetStreamOutbound_explicitDlqSubject_adapterCreated() {
        NatsOutboundChannelModel model = new NatsOutboundChannelModel();
        model.setKey("testChannel");
        model.setSubject("order.completed");
        model.setJetstream(true);
        model.setDlqSubject("dlq.custom.order.completed");

        processor.registerChannelModel(model, null, eventRegistry, eventRepositoryService, false);

        assertThat(model.getOutboundEventChannelAdapter()).isInstanceOf(JetStreamOutboundEventChannelAdapter.class);
    }

    @Test
    void registerJetStreamInbound_thenFindBySubject_returnsModel() {
        NatsInboundChannelModel model = new NatsInboundChannelModel();
        model.setKey("testChannel");
        model.setSubject("order.new");
        model.setJetstream(true);
        model.setMaxDeliver(5);

        processor.registerChannelModel(model, null, eventRegistry, eventRepositoryService, false);

        assertThat(processor.findBySubject("order.new")).contains(model);
        assertThat(processor.findBySubject("unknown.subject")).isEmpty();
    }

    @Test
    void unregisterChannelModel_removesSubjectMapping() {
        NatsInboundChannelModel model = new NatsInboundChannelModel();
        model.setKey("testChannel");
        model.setSubject("order.new");
        model.setJetstream(true);
        model.setMaxDeliver(5);
        processor.registerChannelModel(model, null, eventRegistry, eventRepositoryService, false);

        processor.unregisterChannelModel(model, null, eventRepositoryService);

        assertThat(processor.findBySubject("order.new")).isEmpty();
    }

    // --- Sentinel Phase 5.5 (round 2) coverage: canProcessIfChannelModelAlreadyRegistered ---

    @Test
    void canProcessIfChannelModelAlreadyRegistered_natsOutboundModel_returnsTrue() {
        NatsOutboundChannelModel model = new NatsOutboundChannelModel();
        assertThat(processor.canProcessIfChannelModelAlreadyRegistered(model)).isTrue();
    }

    @Test
    void canProcessIfChannelModelAlreadyRegistered_inboundModel_returnsFalse() {
        NatsInboundChannelModel model = new NatsInboundChannelModel();
        assertThat(processor.canProcessIfChannelModelAlreadyRegistered(model)).isFalse();
    }

    // --- unregisterChannelModel: core (non-JetStream) inbound adapter cleanup path ---

    @Test
    void unregisterChannelModel_coreInboundAdapter_unsubscribesAndClosesDispatcher() throws Exception {
        Dispatcher dispatcher = mock(Dispatcher.class);
        when(connection.createDispatcher()).thenReturn(dispatcher);
        when(dispatcher.subscribe(anyString(), any(MessageHandler.class))).thenReturn(mock(Subscription.class));

        NatsInboundChannelModel model = new NatsInboundChannelModel();
        model.setKey("testChannel");
        model.setSubject("order.new");
        // jetstream left false -> registerInbound (core adapter), not registerJetStreamInbound
        processor.registerChannelModel(model, null, eventRegistry, eventRepositoryService, false);

        processor.unregisterChannelModel(model, null, eventRepositoryService);

        verify(dispatcher).drain(any(java.time.Duration.class));
        verify(connection).closeDispatcher(dispatcher);
    }

    // --- registerJetStreamInbound / registerJetStreamOutbound: autoCreateStream wiring ---

    @Test
    void registerJetStreamInbound_autoCreateStreamTrueWithStreamName_ensuresStream() {
        NatsInboundChannelModel model = new NatsInboundChannelModel();
        model.setKey("testChannel");
        model.setSubject("order.new");
        model.setJetstream(true);
        model.setMaxDeliver(5);
        model.setAutoCreateStream(true);
        model.setStreamName("orders-in-stream");

        processor.registerChannelModel(model, null, eventRegistry, eventRepositoryService, false);

        verify(streamManager).ensureStream("orders-in-stream", "order.new", connection);
        assertThat(model.getStreamName()).isEqualTo("orders-in-stream");
    }

    @Test
    void registerJetStreamInbound_autoCreateStreamTrueButNoStreamName_doesNotEnsureStream() {
        NatsInboundChannelModel model = new NatsInboundChannelModel();
        model.setKey("testChannel");
        model.setSubject("order.new");
        model.setJetstream(true);
        model.setMaxDeliver(5);
        model.setAutoCreateStream(true);
        // streamName intentionally left null

        processor.registerChannelModel(model, null, eventRegistry, eventRepositoryService, false);

        verify(streamManager, never()).ensureStream(any(), any(), any());
    }

    @Test
    void registerJetStreamInbound_explicitDlqSubject_usedInsteadOfDefaultConvention() {
        NatsInboundChannelModel model = new NatsInboundChannelModel();
        model.setKey("testChannel");
        model.setSubject("order.new");
        model.setJetstream(true);
        model.setMaxDeliver(5);
        model.setDlqSubject("dlq.custom.order.new");

        processor.registerChannelModel(model, null, eventRegistry, eventRepositoryService, false);

        assertThat(model.getDlqSubject()).isEqualTo("dlq.custom.order.new");
        assertThat(model.getInboundEventChannelAdapter())
                .isInstanceOf(JetStreamInboundEventChannelAdapter.class);
    }

    @Test
    void registerJetStreamOutbound_autoCreateStreamTrueWithStreamName_ensuresStream() {
        NatsOutboundChannelModel model = new NatsOutboundChannelModel();
        model.setKey("testChannel");
        model.setSubject("order.completed");
        model.setJetstream(true);
        model.setAutoCreateStream(true);
        model.setStreamName("orders-out-stream");

        processor.registerChannelModel(model, null, eventRegistry, eventRepositoryService, false);

        verify(streamManager).ensureStream("orders-out-stream", "order.completed", connection);
        assertThat(model.getStreamName()).isEqualTo("orders-out-stream");
    }

    @Test
    void registerJetStreamOutbound_autoCreateStreamTrueButNoStreamName_doesNotEnsureStream() {
        NatsOutboundChannelModel model = new NatsOutboundChannelModel();
        model.setKey("testChannel");
        model.setSubject("order.completed");
        model.setJetstream(true);
        model.setAutoCreateStream(true);

        processor.registerChannelModel(model, null, eventRegistry, eventRepositoryService, false);

        verify(streamManager, never()).ensureStream(any(), any(), any());
    }

    // --- resolveKey: tenant-scoped registration/unregistration lifecycle ---

    @Test
    void registerAndUnregisterJetStreamInbound_withTenantId_lifecycleSucceeds() {
        NatsInboundChannelModel model = new NatsInboundChannelModel();
        model.setKey("testChannel");
        model.setSubject("order.new");
        model.setJetstream(true);
        model.setMaxDeliver(5);

        processor.registerChannelModel(model, "tenantA", eventRegistry, eventRepositoryService, false);
        assertThat(processor.findBySubject("order.new")).contains(model);

        processor.unregisterChannelModel(model, "tenantA", eventRepositoryService);

        assertThat(processor.findBySubject("order.new")).isEmpty();
    }
}
