package com.threeai.nats.core.outbound;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;

class OutboundClassificationPropertiesTest {

    @Test
    void classify_defaultConfiguration_everyTypeIsBestEffort() {
        OutboundClassificationProperties properties = new OutboundClassificationProperties();

        assertThat(properties.classify("order.created")).isEqualTo(OutboundClassification.BEST_EFFORT);
    }

    @Test
    void classify_typeInCriticalSet_returnsCritical() {
        OutboundClassificationProperties properties = new OutboundClassificationProperties();
        properties.setCriticalTypes(Set.of("payment.requested"));

        assertThat(properties.classify("payment.requested")).isEqualTo(OutboundClassification.CRITICAL);
        assertThat(properties.classify("order.created")).isEqualTo(OutboundClassification.BEST_EFFORT);
        // getCriticalTypes() is what a Spring-bound consumer/inspector reads back -- must be the
        // SAME set classify() itself consults, not a decoupled copy.
        assertThat(properties.getCriticalTypes()).containsExactly("payment.requested");
    }

    @Test
    void variableAllowlistFor_unconfiguredType_returnsEmptyList() {
        OutboundClassificationProperties properties = new OutboundClassificationProperties();

        assertThat(properties.variableAllowlistFor("order.created")).isEmpty();
    }

    @Test
    void variableAllowlistFor_configuredType_returnsConfiguredList() {
        OutboundClassificationProperties properties = new OutboundClassificationProperties();
        properties.setVariableAllowlist(Map.of("order.created", List.of("amount", "currency")));

        assertThat(properties.variableAllowlistFor("order.created")).containsExactly("amount", "currency");
        assertThat(properties.getVariableAllowlist()).containsEntry("order.created", List.of("amount", "currency"));
    }
}
