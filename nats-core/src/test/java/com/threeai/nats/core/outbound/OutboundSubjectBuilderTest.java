package com.threeai.nats.core.outbound;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.threeai.nats.core.exception.InvalidOutboundMessageTypeException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class OutboundSubjectBuilderTest {

    @Test
    void build_validArgs_buildsInstanceKeyedSubject() {
        String subject = OutboundSubjectBuilder.build("camunda", "order_created", "proc-1");

        assertThat(subject).isEqualTo("events.camunda.order_created.proc-1");
    }

    @Test
    void build_sameProcessInstance_producesSameSubject_sequencePreserved() {
        String first = OutboundSubjectBuilder.build("camunda", "order_created", "proc-1");
        String second = OutboundSubjectBuilder.build("camunda", "order_shipped", "proc-1");

        // D-E': instance-keyed -- both messages for the SAME instance share the third+ path
        // only when the type also matches; different types intentionally land on different
        // subjects, but both start with the same engine segment and end with the same instance id.
        assertThat(first).startsWith("events.camunda.").endsWith(".proc-1");
        assertThat(second).startsWith("events.camunda.").endsWith(".proc-1");
    }

    @Test
    void build_nullEngineId_throws() {
        assertThatThrownBy(() -> OutboundSubjectBuilder.build(null, "order_created", "proc-1"))
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
        assertThatThrownBy(() -> OutboundSubjectBuilder.build("camunda", "order_created", null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("processInstanceId");
    }

    // --- Phase-review FINDING-003 (MINOR — messageType subject-token safety) ---

    @ParameterizedTest
    @ValueSource(strings = {"a.b", "x*", "y>z", "a b"})
    void build_messageTypeUnsafeForSubjectToken_throwsInvalidOutboundMessageTypeException(String unsafeMessageType) {
        assertThatThrownBy(() -> OutboundSubjectBuilder.build("camunda", unsafeMessageType, "proc-1"))
                .isInstanceOf(InvalidOutboundMessageTypeException.class)
                .hasMessageContaining("VAL_OUTBOUND_MESSAGE_TYPE_INVALID");
    }

    @Test
    void build_messageTypeWithDot_wouldOtherwiseAddExtraSubjectSegment_isRejected() {
        // "a.b" would silently produce a 5-segment subject (events.camunda.a.b.proc-1) instead of
        // the intended 4-segment events.<engineId>.<type>.<processInstanceId> shape -- rejected,
        // not silently accepted.
        assertThatThrownBy(() -> OutboundSubjectBuilder.build("camunda", "a.b", "proc-1"))
                .isInstanceOf(InvalidOutboundMessageTypeException.class);
    }

    @Test
    void build_messageTypeWithNatsWildcards_isRejected() {
        assertThatThrownBy(() -> OutboundSubjectBuilder.build("camunda", "x*", "proc-1"))
                .isInstanceOf(InvalidOutboundMessageTypeException.class);
        assertThatThrownBy(() -> OutboundSubjectBuilder.build("camunda", "y>z", "proc-1"))
                .isInstanceOf(InvalidOutboundMessageTypeException.class);
    }

    @Test
    void build_messageTypeWithWhitespace_isRejected() {
        assertThatThrownBy(() -> OutboundSubjectBuilder.build("camunda", "a b", "proc-1"))
                .isInstanceOf(InvalidOutboundMessageTypeException.class);
    }

    @Test
    void build_exceptionCarriesCodeAndOffendingMessageType() {
        assertThatThrownBy(() -> OutboundSubjectBuilder.build("camunda", "a.b", "proc-1"))
                .isInstanceOfSatisfying(InvalidOutboundMessageTypeException.class, e -> {
                    assertThat(e.getCode()).isEqualTo("VAL_OUTBOUND_MESSAGE_TYPE_INVALID");
                    assertThat(e.getMessageType()).isEqualTo("a.b");
                });
    }

    @ParameterizedTest
    @ValueSource(strings = {"orderCreated", "order_created", "order-created", "ORDER123", "a"})
    void build_validMessageTypes_produceCorrectSubject(String validMessageType) {
        String subject = OutboundSubjectBuilder.build("camunda", validMessageType, "proc-1");

        assertThat(subject).isEqualTo("events.camunda." + validMessageType + ".proc-1");
    }
}
