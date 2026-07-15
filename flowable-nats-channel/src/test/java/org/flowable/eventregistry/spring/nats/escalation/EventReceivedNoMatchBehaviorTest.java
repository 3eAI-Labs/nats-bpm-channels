package org.flowable.eventregistry.spring.nats.escalation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.mock;

import java.util.Collection;
import java.util.List;
import java.util.Map;

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
 * TEST_SPECIFICATIONS.md (d) — {@code eventReceived} no-match behavior (HLD §11 item 6c,
 * BR-FLW-005, US-B5). Characterizes what {@link EventRegistry#eventReceived} actually does when
 * there is no waiting event-subscription for the incoming correlation key — the LLD's open
 * question that {@link FailureEventBridge}'s {@code catch (FlowableException noMatch)} branch
 * (line ~142) was written against speculatively (CODER-QUESTION, {@code 03_classes/4_flowable.md}
 * §2).
 *
 * <p><b>Method:</b> a REAL embedded Flowable {@code ProcessEngine} (H2 in-memory) is built so the
 * REAL {@code BpmnEventRegistryEventConsumer} participates — this consumer is auto-registered
 * during normal engine bootstrap ({@code ProcessEngineConfigurationImpl.disableEventRegistry}
 * defaults to {@code false}; {@code flowable-event-registry-configurator} is already a transitive
 * "provided" dependency of {@code flowable-engine}, verified via {@code mvn dependency:tree}) — it
 * is the actual consumer a {@code FailureEventBridge}-forwarded failure-event reaches in
 * production. No BPMN process is deployed and none is started, so {@code ACT_RU_EVENT_SUBSCR} is
 * guaranteed empty for ANY correlation key — i.e. "no waiting subscription" is the only possible
 * outcome, deterministically, without needing to race an interrupting-escalation window.
 *
 * <p><b>Bytecode evidence (javap -c against the compiled 7.1.0 engine jars — no source jar is
 * published for this artifact):</b>
 * <ul>
 *   <li>{@code BpmnEventRegistryEventConsumer.eventReceived(EventInstance)} calls
 *       {@code findEventSubscriptions("bpmn", event, correlationKeys)} -&gt;
 *       {@code List<EventSubscription>}, then iterates that list calling
 *       {@code handleEventSubscription(...)} once per entry. If the list is EMPTY, the loop body
 *       never executes — no exception path exists in the bytecode for that case; an EMPTY
 *       {@code EventRegistryProcessingInfo} is simply returned.</li>
 *   <li>{@code BaseEventRegistryEventConsumer.eventReceived(EventRegistryEvent)} (the public
 *       entry point {@code sendEventToConsumers} calls) has NO try/catch around its delegation to
 *       the abstract {@code eventReceived(EventInstance)} — nothing upstream converts a "no
 *       match" outcome into an exception either.</li>
 *   <li>{@code findEventSubscriptions} itself is a plain
 *       {@code EventSubscriptionQuery...list()} call — an empty result list is not a query error.</li>
 * </ul>
 *
 * <p><b>Conclusion (resolves the LLD's open question):</b> {@code eventReceived(...)} does
 * <b>not</b> throw {@code FlowableException} (or any exception) for the "no waiting subscription"
 * case — it returns silently. {@link FailureEventBridge}'s
 * {@code catch (FlowableException noMatch)} branch is therefore <b>dead code</b> for this
 * specific scenario: a genuine correlation-miss will fall through to the
 * {@code catch (Exception downstreamFailure)} branch instead (if the pipeline component throws
 * for some other config reason) or, per this test, not throw at all and simply return — meaning
 * {@code RES_FAILURE_EVENT_CORRELATION_MISS} (BAQ-8, the metric/WARN this branch was meant to
 * record) is currently <b>never incremented</b> by a real no-match event, only by the synthetic
 * {@code FlowableException} the existing unit test ({@code FailureEventBridgeTest
 * .handleDlqMessage_noMatchingSubscription_acksWithCorrelationMissMetric}) injects via
 * {@code doThrow(...)}. This is reported as a QA finding (Sentinel Phase 5.5), not fixed here
 * (test-only scope) — see the phase report's DECISION_MATRIX/TEST_SPEC section.
 */
class EventReceivedNoMatchBehaviorTest {

    private ProcessEngine processEngine;

    @BeforeEach
    void setUp() {
        ProcessEngineConfigurationImpl configuration = (ProcessEngineConfigurationImpl)
                ProcessEngineConfiguration.createStandaloneInMemProcessEngineConfiguration();
        configuration.setJdbcUrl("jdbc:h2:mem:event-no-match-test-" + System.nanoTime() + ";DB_CLOSE_DELAY=-1");
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
    void eventReceived_noWaitingSubscription_doesNotThrow_returnsSilently() {
        EventRegistry eventRegistry = eventRegistry();

        InboundChannelModel channelModel = new InboundChannelModel();
        channelModel.setKey("no-match-channel");
        channelModel.setInboundEventProcessingPipeline(new SingleEventPassthroughPipeline());

        InboundEvent rawEvent = mock(InboundEvent.class);

        // Fresh engine, nothing deployed/started -> ACT_RU_EVENT_SUBSCR is empty for ANY
        // correlation key -> the real BpmnEventRegistryEventConsumer is guaranteed to find zero
        // matching subscriptions, deterministically (this IS the "no waiting subscription"
        // scenario BR-FLW-005/US-B5 describes, not a simulation of it).
        assertThatCode(() -> eventRegistry.eventReceived(channelModel, rawEvent))
                .doesNotThrowAnyException();
    }

    private EventRegistry eventRegistry() {
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
        return eventRegistryEngineConfiguration.getEventRegistry();
    }

    /**
     * Bypasses real deserialization entirely — hands the dispatch layer (the layer under test) a
     * fabricated {@link EventInstance} directly, so the test does not depend on deploying a
     * channel/event JSON definition (orthogonal to the question this test answers).
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
