package com.threeai.nats.camunda.eventing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.camunda.bpm.engine.impl.persistence.entity.DeploymentEntity;
import org.camunda.bpm.engine.impl.persistence.entity.ResourceEntity;
import org.junit.jupiter.api.Test;

/** docs/12 G1: tx-safe trigger — validate-or-fail before broker contact, nudge only on commit path. */
class EventingDeployerTest {

    private final List<Runnable> hooked = new ArrayList<>();
    private int nudges;
    private final EventingDeployer deployer =
            new EventingDeployer(() -> nudges++, hooked::add);

    private static DeploymentEntity deployment(Map<String, String> resources) {
        Map<String, ResourceEntity> entities = new LinkedHashMap<>();
        for (Map.Entry<String, String> e : resources.entrySet()) {
            ResourceEntity r = mock(ResourceEntity.class);
            when(r.getBytes()).thenReturn(e.getValue().getBytes(StandardCharsets.UTF_8));
            entities.put(e.getKey(), r);
        }
        DeploymentEntity d = mock(DeploymentEntity.class);
        when(d.getResources()).thenReturn(entities);
        when(d.getId()).thenReturn("d1");
        return d;
    }

    @Test
    void validDefinitions_registerPostCommitNudge_withoutRunningIt() {
        deployer.deploy(deployment(Map.of(
                "order.event", "{ \"key\": \"orderEvent\" }",
                "order.channel", """
                        { "key": "c", "channelType": "inbound", "type": "nats",
                          "channelEventKeyDetection": { "fixedValue": "orderEvent" },
                          "queueGroup": "g", "subject": "order.new" }
                        """)));

        assertThat(hooked).hasSize(1);
        assertThat(nudges).isZero();          // nothing before commit
        hooked.get(0).run();                   // engine fires COMMITTED
        assertThat(nudges).isEqualTo(1);
    }

    @Test
    void invalidDefinition_throws_beforeAnyHook() {
        assertThatThrownBy(() -> deployer.deploy(deployment(Map.of(
                "bad.channel", "{ \"key\": \"c\" }"))))
                .isInstanceOf(EventingDefinitionException.class);
        assertThat(hooked).isEmpty();
    }

    @Test
    void bpmnOnlyReinvocation_isSilent_noNudge() {
        deployer.deploy(deployment(Map.of("process.bpmn20.xml", "<definitions/>")));
        assertThat(hooked).isEmpty();
        assertThat(nudges).isZero();
    }

    @Test
    void nullOrEmptyResources_areSilent() {
        DeploymentEntity d = mock(DeploymentEntity.class);
        when(d.getResources()).thenReturn(null);
        deployer.deploy(d);
        assertThat(hooked).isEmpty();
    }
}
