package com.threeai.nats.cibseven.a2;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.Test;

class A2TopicConfigTest {

    @Test
    void isA2Topic_configuredTopic_returnsTrue() {
        A2Properties properties = new A2Properties();
        properties.setTopics(List.of("order-fulfillment", "payment-capture"));
        A2TopicConfig config = new A2TopicConfig(properties);

        assertThat(config.isA2Topic("order-fulfillment")).isTrue();
        assertThat(config.isA2Topic("payment-capture")).isTrue();
        assertThat(config.isA2Topic("unknown-topic")).isFalse();
    }

    @Test
    void a2Topics_isImmutable() {
        A2Properties properties = new A2Properties();
        properties.setTopics(List.of("order-fulfillment"));
        A2TopicConfig config = new A2TopicConfig(properties);

        assertThat(config.a2Topics()).containsExactly("order-fulfillment");
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> config.a2Topics().add("x"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void a2Topics_emptyByDefault() {
        A2TopicConfig config = new A2TopicConfig(new A2Properties());

        assertThat(config.a2Topics()).isEmpty();
    }

    /** Sentinel Phase 5.5 QA fix (item 5) — default variableAllowlist is empty for any topic. */
    @Test
    void variableAllowlistFor_unconfiguredTopic_returnsEmptyList() {
        A2Properties properties = new A2Properties();
        properties.setTopics(List.of("order-fulfillment"));
        A2TopicConfig config = new A2TopicConfig(properties);

        assertThat(config.variableAllowlistFor("order-fulfillment")).isEmpty();
        assertThat(config.variableAllowlistFor("never-configured")).isEmpty();
    }

    @Test
    void variableAllowlistFor_configuredTopic_returnsConfiguredNames() {
        A2Properties properties = new A2Properties();
        properties.setTopics(List.of("payment-capture"));
        properties.setVariableAllowlist(java.util.Map.of(
                "payment-capture", List.of("amount", "currency")));
        A2TopicConfig config = new A2TopicConfig(properties);

        assertThat(config.variableAllowlistFor("payment-capture")).containsExactly("amount", "currency");
    }
}
