package com.threeai.nats.camunda.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import org.camunda.bpm.engine.ProcessEngine;
import org.camunda.bpm.engine.RepositoryService;
import org.camunda.bpm.engine.RuntimeService;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import com.threeai.nats.camunda.eventing.EventingDeployer;
import com.threeai.nats.camunda.eventing.EventingReconcileScheduler;
import com.threeai.nats.camunda.eventing.EventingRegistry;
import com.threeai.nats.camunda.eventing.OverlayedOutboundClassification;
import com.threeai.nats.core.dlq.DlqPublisher;
import com.threeai.nats.core.jetstream.JetStreamStreamManager;
import com.threeai.nats.core.outbound.OutboundClassificationProperties;

import io.nats.client.Connection;
import io.nats.client.JetStream;

/** D-F v2: the gate defaults CLOSED; open, the full chain wires and the overlay is @Primary. */
class CamundaEventingAutoConfigurationTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(CamundaEventingAutoConfiguration.class))
            .withBean(Connection.class, () -> mock(Connection.class))
            .withBean(JetStream.class, () -> mock(JetStream.class))
            .withBean(JetStreamStreamManager.class, () -> mock(JetStreamStreamManager.class))
            .withBean(RuntimeService.class, () -> mock(RuntimeService.class))
            .withBean(RepositoryService.class, () -> mock(RepositoryService.class))
            .withBean(ProcessEngine.class, () -> mock(ProcessEngine.class))
            .withBean(DlqPublisher.class, () -> mock(DlqPublisher.class))
            .withBean(OutboundClassificationProperties.class, OutboundClassificationProperties::new);

    @Test
    void gateClosedByDefault_capabilityIsCompletelyInert() {
        runner.run(context -> {
            assertThat(context).doesNotHaveBean(EventingRegistry.class);
            assertThat(context).doesNotHaveBean(EventingDeployer.class);
            assertThat(context).doesNotHaveBean(EventingReconcileScheduler.class);
            assertThat(context).doesNotHaveBean(OverlayedOutboundClassification.class);
        });
    }

    @Test
    void gateOpen_wiresDeployerPluginReconcilerAndPrimaryOverlay() {
        runner.withPropertyValues("spring.nats.camunda.eventing.enabled=true")
                .run(context -> {
                    assertThat(context).hasSingleBean(EventingRegistry.class);
                    assertThat(context).hasSingleBean(EventingDeployer.class);
                    assertThat(context).hasSingleBean(EventingReconcileScheduler.class);
                    assertThat(context).hasBean("eventingProcessEnginePlugin");
                    // F-7: injection by the BASE type resolves to the engine-scoped overlay
                    assertThat(context.getBean(OutboundClassificationProperties.class))
                            .isInstanceOf(OverlayedOutboundClassification.class);
                });
    }

    @Test
    void gateOpen_unresolvableLocationFailsStartup_emptyWildcardDoesNot() {
        // rejection over silence: a classpath: root that does not exist is an operator typo
        runner.withPropertyValues("spring.nats.camunda.eventing.enabled=true",
                        "spring.nats.camunda.eventing.definitions.locations=classpath:does-not-exist/*.event")
                .run(context -> assertThat(context).hasFailed());
        // classpath*: sweeps are allowed to match nothing (the documented multi-jar form)
        runner.withPropertyValues("spring.nats.camunda.eventing.enabled=true",
                        "spring.nats.camunda.eventing.definitions.locations=classpath*:does-not-exist/*.event")
                .run(context -> assertThat(context).hasNotFailed());
    }
}
