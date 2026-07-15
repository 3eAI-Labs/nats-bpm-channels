package com.threeai.nats.core.config;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.threeai.nats.core.exception.TopicNamespaceCollisionException;
import org.junit.jupiter.api.Test;

class NamespaceValidatorTest {

    @Test
    void assertNotReservedForA2_reservedPrefix_throws() {
        assertThatThrownBy(() -> NamespaceValidator.assertNotReservedForA2("jobs.order-fulfillment", "orderChannel"))
                .isInstanceOf(TopicNamespaceCollisionException.class)
                .hasMessageContaining("jobs.order-fulfillment")
                .hasMessageContaining("orderChannel");
    }

    @Test
    void assertNotReservedForA2_notReserved_doesNotThrow() {
        assertThatCode(() -> NamespaceValidator.assertNotReservedForA2("order.new", "orderChannel"))
                .doesNotThrowAnyException();
    }

    @Test
    void assertNotReservedForA2_nullSubject_doesNotThrow() {
        assertThatCode(() -> NamespaceValidator.assertNotReservedForA2(null, "orderChannel"))
                .doesNotThrowAnyException();
    }

    @Test
    void exception_carriesCodeAndContext() {
        try {
            NamespaceValidator.assertNotReservedForA2("jobs.foo", "fooChannel");
        } catch (TopicNamespaceCollisionException e) {
            org.assertj.core.api.Assertions.assertThat(e.getCode()).isEqualTo("VAL_TOPIC_NAMESPACE_COLLISION");
            org.assertj.core.api.Assertions.assertThat(e.getSubject()).isEqualTo("jobs.foo");
            org.assertj.core.api.Assertions.assertThat(e.getChannelKey()).isEqualTo("fooChannel");
        }
    }
}
