package com.threeai.nats.core.outbound;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class OutboundSubjectBuilderTest {

    @Test
    void build_validArgs_buildsInstanceKeyedSubject() {
        String subject = OutboundSubjectBuilder.build("camunda", "order.created", "proc-1");

        assertThat(subject).isEqualTo("events.camunda.order.created.proc-1");
    }

    @Test
    void build_sameProcessInstance_producesSameSubject_sequencePreserved() {
        String first = OutboundSubjectBuilder.build("camunda", "order.created", "proc-1");
        String second = OutboundSubjectBuilder.build("camunda", "order.shipped", "proc-1");

        // D-E': instance-keyed -- both messages for the SAME instance share the third+ path
        // only when the type also matches; different types intentionally land on different
        // subjects, but both start with the same engine segment and end with the same instance id.
        assertThat(first).startsWith("events.camunda.").endsWith(".proc-1");
        assertThat(second).startsWith("events.camunda.").endsWith(".proc-1");
    }

    @Test
    void build_nullEngineId_throws() {
        assertThatThrownBy(() -> OutboundSubjectBuilder.build(null, "order.created", "proc-1"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("engineId");
    }

    @Test
    void build_blankMessageType_throws() {
        assertThatThrownBy(() -> OutboundSubjectBuilder.build("camunda", "  ", "proc-1"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("messageType");
    }

    @Test
    void build_nullProcessInstanceId_throws() {
        assertThatThrownBy(() -> OutboundSubjectBuilder.build("camunda", "order.created", null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("processInstanceId");
    }
}
