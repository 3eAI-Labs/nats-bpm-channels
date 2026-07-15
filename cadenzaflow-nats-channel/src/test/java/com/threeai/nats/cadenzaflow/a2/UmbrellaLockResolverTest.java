package com.threeai.nats.cadenzaflow.a2;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class UmbrellaLockResolverTest {

    @Test
    void resolveMillis_defaultParameters_producesAdrCanonicalExample() {
        A2Properties properties = new A2Properties();
        properties.setTopics(java.util.List.of("order-fulfillment"));
        UmbrellaLockResolver resolver = new UmbrellaLockResolver(properties);

        // ADR-0001 canonical example: W=30,M=4,S=120,eps=60 -> floor=307, L=320
        assertThat(resolver.resolveSeconds("order-fulfillment")).isEqualTo(320);
        assertThat(resolver.resolveMillis("order-fulfillment")).isEqualTo(320_000L);
    }

    @Test
    void resolveSeconds_topicOverrideAckWait_rederivesL() {
        A2Properties properties = new A2Properties();
        properties.setTopics(java.util.List.of("payment-capture"));
        A2Properties.TopicLockOverride override = new A2Properties.TopicLockOverride();
        override.setAckWaitSeconds(90L);
        properties.setTopicOverrides(java.util.Map.of("payment-capture", override));
        UmbrellaLockResolver resolver = new UmbrellaLockResolver(properties);

        // floor = 4*90 + backoffSum(4)=7 + 120 + 60 = 360+7+180 = 547; L = 547+13 = 560
        assertThat(resolver.resolveSeconds("payment-capture")).isEqualTo(560);
    }

    @Test
    void resolveSeconds_manualLockDurationOverride_usedVerbatim() {
        A2Properties properties = new A2Properties();
        properties.setTopics(java.util.List.of("custom-topic"));
        A2Properties.TopicLockOverride override = new A2Properties.TopicLockOverride();
        override.setLockDurationSeconds(999L);
        properties.setTopicOverrides(java.util.Map.of("custom-topic", override));
        UmbrellaLockResolver resolver = new UmbrellaLockResolver(properties);

        assertThat(resolver.resolveSeconds("custom-topic")).isEqualTo(999);
    }

    @Test
    void resolveSeconds_isCachedAfterFirstCall() {
        A2Properties properties = new A2Properties();
        properties.setTopics(java.util.List.of("order-fulfillment"));
        UmbrellaLockResolver resolver = new UmbrellaLockResolver(properties);

        long first = resolver.resolveSeconds("order-fulfillment");
        // Mutate properties after first resolution — cached value must NOT change.
        properties.getDefaults().setAckWaitSeconds(999);
        long second = resolver.resolveSeconds("order-fulfillment");

        assertThat(second).isEqualTo(first);
    }
}
