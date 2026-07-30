package com.threeai.nats.cibseven.a2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;

import com.threeai.nats.core.exception.UmbrellaLockConfigurationException;
import org.junit.jupiter.api.Test;

class UmbrellaLockValidatorTest {

    @Test
    void afterPropertiesSet_defaultDerivedL_passesValidation() {
        A2Properties properties = new A2Properties();
        properties.setTopics(List.of("order-fulfillment"));
        UmbrellaLockResolver resolver = new UmbrellaLockResolver(properties);
        UmbrellaLockValidator validator = new UmbrellaLockValidator(properties, resolver);

        assertThatCode(validator::afterPropertiesSet).doesNotThrowAnyException();
        assertThat(validator.isUnsafe("order-fulfillment")).isFalse();
    }

    @Test
    void afterPropertiesSet_lTooShort_rejectsStartupByDefault() {
        A2Properties properties = new A2Properties();
        properties.setTopics(List.of("order-fulfillment"));
        A2Properties.TopicLockOverride override = new A2Properties.TopicLockOverride();
        override.setLockDurationSeconds(1L); // far below the floor
        properties.setTopicOverrides(Map.of("order-fulfillment", override));
        UmbrellaLockResolver resolver = new UmbrellaLockResolver(properties);
        UmbrellaLockValidator validator = new UmbrellaLockValidator(properties, resolver);

        assertThatThrownBy(validator::afterPropertiesSet)
                .isInstanceOf(UmbrellaLockConfigurationException.class);
    }

    @Test
    void afterPropertiesSet_lTooShortWithEscapeFlag_marksTopicUnsafeInsteadOfRejecting() {
        A2Properties properties = new A2Properties();
        properties.setTopics(List.of("order-fulfillment"));
        properties.setAllowUnsafeLockDuration(true);
        A2Properties.TopicLockOverride override = new A2Properties.TopicLockOverride();
        override.setLockDurationSeconds(1L);
        properties.setTopicOverrides(Map.of("order-fulfillment", override));
        UmbrellaLockResolver resolver = new UmbrellaLockResolver(properties);
        UmbrellaLockValidator validator = new UmbrellaLockValidator(properties, resolver);

        assertThatCode(validator::afterPropertiesSet).doesNotThrowAnyException();
        assertThat(validator.isUnsafe("order-fulfillment")).isTrue();
    }

    @Test
    void isUnsafe_topicNeverValidated_returnsFalse() {
        A2Properties properties = new A2Properties();
        UmbrellaLockValidator validator = new UmbrellaLockValidator(properties, new UmbrellaLockResolver(properties));

        assertThat(validator.isUnsafe("never-configured")).isFalse();
    }
}
