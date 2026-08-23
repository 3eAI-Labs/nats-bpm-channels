package com.threeai.nats.camunda.eventing;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.threeai.nats.core.outbound.OutboundClassification;
import com.threeai.nats.core.outbound.OutboundClassificationProperties;

/** F-7 (docs/12 D-D v2): overlay-first reads, YAML fallback, revert on removal. */
class OverlayedOutboundClassificationTest {

    private OutboundClassificationProperties base;
    private OverlayedOutboundClassification overlay;

    @BeforeEach
    void setUp() {
        base = new OutboundClassificationProperties();
        base.setEnabled(true);
        base.setCriticalTypes(Set.of("yaml-critical"));
        base.setVariableAllowlist(Map.of("yaml-critical", List.of("fromYaml")));
        overlay = new OverlayedOutboundClassification(base);
    }

    @Test
    void withoutOverlay_delegatesToYamlBase() {
        assertThat(overlay.classify("yaml-critical")).isEqualTo(OutboundClassification.CRITICAL);
        assertThat(overlay.classify("unknown")).isEqualTo(OutboundClassification.BEST_EFFORT);
        assertThat(overlay.variableAllowlistFor("yaml-critical")).containsExactly("fromYaml");
        assertThat(overlay.isEnabled()).isTrue();
    }

    @Test
    void overlayWinsForCoveredKeys_yamlForOthers() {
        overlay.swapOverlay(Map.of(
                "order-created", new OutboundOverlayDefinition("k1", "order-created", true, List.of("orderId")),
                "yaml-critical", new OutboundOverlayDefinition("k2", "yaml-critical", false, List.of())));

        assertThat(overlay.classify("order-created")).isEqualTo(OutboundClassification.CRITICAL);
        assertThat(overlay.variableAllowlistFor("order-created")).containsExactly("orderId");
        // a covered key is FULLY defined by the definition — YAML criticality is overridden
        assertThat(overlay.classify("yaml-critical")).isEqualTo(OutboundClassification.BEST_EFFORT);
        assertThat(overlay.variableAllowlistFor("yaml-critical")).isEmpty();
        // uncovered keys keep the YAML base
        assertThat(overlay.classify("unknown")).isEqualTo(OutboundClassification.BEST_EFFORT);
    }

    @Test
    void removalRevertsToYaml() {
        overlay.swapOverlay(Map.of(
                "yaml-critical", new OutboundOverlayDefinition("k2", "yaml-critical", false, List.of())));
        assertThat(overlay.classify("yaml-critical")).isEqualTo(OutboundClassification.BEST_EFFORT);

        overlay.swapOverlay(Map.of()); // definition removed -> revert (D-D v2)

        assertThat(overlay.classify("yaml-critical")).isEqualTo(OutboundClassification.CRITICAL);
        assertThat(overlay.variableAllowlistFor("yaml-critical")).containsExactly("fromYaml");
    }

    @Test
    void mutatorsDelegate_subclassStateNeverUsed() {
        overlay.setEnabled(false);
        assertThat(base.isEnabled()).isFalse();
        assertThat(overlay.getCriticalTypes()).isSameAs(base.getCriticalTypes());
        assertThat(overlay.getVariableAllowlist()).isSameAs(base.getVariableAllowlist());
    }
}
