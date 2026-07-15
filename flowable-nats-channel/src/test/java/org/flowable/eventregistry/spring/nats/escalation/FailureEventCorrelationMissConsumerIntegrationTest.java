package org.flowable.eventregistry.spring.nats.escalation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.mock;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.threeai.nats.core.metrics.NatsChannelMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.flowable.common.engine.impl.interceptor.EngineConfigurationConstants;
import org.flowable.engine.ProcessEngine;
import org.flowable.engine.ProcessEngineConfiguration;
import org.flowable.engine.impl.cfg.ProcessEngineConfigurationImpl;
import org.flowable.eventregistry.api.EventRegistry;
import org.flowable.eventregistry.api.EventRegistryEvent;
import org.flowable.eventregistry.api.InboundEvent;
import org.flowable.eventregistry.api.InboundEventProcessingPipeline;
import org.flowable.eventregistry.api.runtime.EventInstance;
import org.flowable.eventregistry.api.runtime.EventPayloadInstance;
import org.flowable.eventregistry.impl.EventRegistryEngineConfiguration;
import org.flowable.eventregistry.model.InboundChannelModel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Sentinel Phase 5.5 QA fix (HIGH, Levent karari 2026-07-15) — end-to-end proof, on the SAME
 * REAL embedded {@code ProcessEngine} infrastructure {@code EventReceivedNoMatchBehaviorTest}
 * uses, that {@link FailureEventCorrelationMissConsumer} — registered onto {@link
 * EventRegistryEngineConfiguration} exactly as {@link FailureEventBridge#subscribe()} does in
 * production — is ACTUALLY invoked by the engine on a genuine "no waiting subscription" event,
 * and increments {@code RES_FAILURE_EVENT_CORRELATION_MISS}. This closes the gap {@code
 * EventReceivedNoMatchBehaviorTest} identified (the old {@code catch (FlowableException)} path
 * was dead code) with a real trigger, not a synthetic {@code doThrow(...)} mock.
 */
class FailureEventCorrelationMissConsumerIntegrationTest {

    private ProcessEngine processEngine;

    @BeforeEach
    void setUp() {
        ProcessEngineConfigurationImpl configuration = (ProcessEngineConfigurationImpl)
                ProcessEngineConfiguration.createStandaloneInMemProcessEngineConfiguration();
        configuration.setJdbcUrl("jdbc:h2:mem:correlation-miss-consumer-test-" + System.nanoTime() + ";DB_CLOSE_DELAY=-1");
        configuration.setDatabaseSchemaUpdate(ProcessEngineConfiguration.DB_SCHEMA_UPDATE_TRUE);
        configuration.setAsyncExecutorActivate(false);
        processEngine = configuration.buildProcessEngine();
    }

    @AfterEach
    void tearDown() {
        if (processEngine != null) {
            processEngine.close();
        }
    }

    @Test
    void eventReceived_noWaitingSubscription_invokesConsumerAndIncrementsMetric() {
        EventRegistryEngineConfiguration eventRegistryEngineConfiguration = eventRegistryEngineConfiguration();
        EventRegistry eventRegistry = eventRegistryEngineConfiguration.getEventRegistry();

        NatsChannelMetrics metrics = new NatsChannelMetrics(new SimpleMeterRegistry());
        FailureEventCorrelationMissConsumer consumer = new FailureEventCorrelationMissConsumer(metrics);
        eventRegistryEngineConfiguration.setNonMatchingEventConsumer(consumer);

        InboundChannelModel channelModel = new InboundChannelModel();
        channelModel.setKey("orderChannel");
        channelModel.setInboundEventProcessingPipeline(new SingleEventPassthroughPipeline());

        InboundEvent rawEvent = mock(InboundEvent.class);

        consumer.bindChannelKeyForCurrentThread(channelModel.getKey());
        try {
            assertThatCode(() -> eventRegistry.eventReceived(channelModel, rawEvent))
                    .doesNotThrowAnyException();
        } finally {
            consumer.clearChannelKeyForCurrentThread();
        }

        assertThat(metrics.failureEventCorrelationMissCount("orderChannel").count())
                .as("a genuine no-match event on a fresh engine (no deployments, no waiting "
                        + "subscriptions) must invoke the registered "
                        + "EventRegistryNonMatchingEventConsumer exactly once")
                .isEqualTo(1.0);
    }

    private EventRegistryEngineConfiguration eventRegistryEngineConfiguration() {
        ProcessEngineConfigurationImpl configuration =
                (ProcessEngineConfigurationImpl) processEngine.getProcessEngineConfiguration();
        Map<String, org.flowable.common.engine.impl.AbstractEngineConfiguration> engineConfigurations =
                configuration.getEngineConfigurations();
        EventRegistryEngineConfiguration eventRegistryEngineConfiguration =
                (EventRegistryEngineConfiguration) engineConfigurations.get(
                        EngineConfigurationConstants.KEY_EVENT_REGISTRY_CONFIG);
        assertThat(eventRegistryEngineConfiguration)
                .as("EventRegistryEngineConfigurator must be auto-wired by default (disableEventRegistry=false)")
                .isNotNull();
        return eventRegistryEngineConfiguration;
    }

    /**
     * Bypasses real deserialization entirely — hands the dispatch layer (the layer under test) a
     * fabricated {@link EventInstance} directly, so the test does not depend on deploying a
     * channel/event JSON definition (orthogonal to the question this test answers). Same
     * technique as {@code EventReceivedNoMatchBehaviorTest}.
     */
    private static final class SingleEventPassthroughPipeline implements InboundEventProcessingPipeline {
        @Override
        public Collection<EventRegistryEvent> run(InboundChannelModel channelModel, InboundEvent event) {
            EventInstance eventInstance = new NoCorrelationEventInstance();
            EventRegistryEvent registryEvent = new EventRegistryEvent() {
                @Override
                public String getType() {
                    return "eventInstance";
                }

                @Override
                public Object getEventObject() {
                    return eventInstance;
                }
            };
            return List.of(registryEvent);
        }
    }

    private static final class NoCorrelationEventInstance implements EventInstance {
        @Override
        public String getEventKey() {
            return "no-match-event";
        }

        @Override
        public Collection<EventPayloadInstance> getPayloadInstances() {
            return List.of();
        }

        @Override
        public Collection<EventPayloadInstance> getHeaderInstances() {
            return List.of();
        }

        @Override
        public Collection<EventPayloadInstance> getCorrelationParameterInstances() {
            return List.of();
        }

        @Override
        public String getTenantId() {
            return null;
        }
    }
}
