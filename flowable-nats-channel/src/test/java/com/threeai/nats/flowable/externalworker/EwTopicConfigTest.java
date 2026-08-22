package com.threeai.nats.flowable.externalworker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** D-H' (docs/11): topic token validation at config load — THROW, not warn. */
class EwTopicConfigTest {

    private static EwProperties propsWith(String... topics) {
        EwProperties p = new EwProperties();
        p.setTopics(List.of(topics));
        return p;
    }

    @Test
    void validTopics_areExposedAsImmutableSet() {
        EwTopicConfig config = new EwTopicConfig(propsWith("order-fulfillment", "sms_send", "Topic9"));

        assertThat(config.topics()).containsExactlyInAnyOrder("order-fulfillment", "sms_send", "Topic9");
        assertThat(config.isDispatchTopic("sms_send")).isTrue();
        assertThat(config.isDispatchTopic("other")).isFalse();
        assertThatThrownBy(() -> config.topics().add("x")).isInstanceOf(UnsupportedOperationException.class);
    }

    /**
     * A dot would split the subject token (ewjobs.<topic>), '*'/'>' would wildcard it, and
     * whitespace breaks both the subject and the flw-ew-completion-<topic> durable name.
     */
    @ParameterizedTest
    @ValueSource(strings = {"a.b", "a*", "a>", "a b", "", "tópic", "a/b"})
    void unsafeTopics_throwAtConstruction(String topic) {
        assertThatThrownBy(() -> new EwTopicConfig(propsWith(topic)))
                .isInstanceOf(InvalidEwTopicException.class)
                .hasMessageContaining("VAL_EW_TOPIC_INVALID");
    }

    @Test
    void nullTopic_throwsAtConstruction() {
        EwProperties p = new EwProperties();
        p.setTopics(java.util.Arrays.asList("ok", null));
        assertThatThrownBy(() -> new EwTopicConfig(p))
                .isInstanceOf(InvalidEwTopicException.class);
    }
}
