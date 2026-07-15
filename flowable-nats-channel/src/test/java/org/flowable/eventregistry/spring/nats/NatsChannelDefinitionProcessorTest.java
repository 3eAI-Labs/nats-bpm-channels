package org.flowable.eventregistry.spring.nats;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
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
}
