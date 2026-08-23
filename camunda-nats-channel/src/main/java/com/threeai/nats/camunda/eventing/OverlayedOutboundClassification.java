package com.threeai.nats.camunda.eventing;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.threeai.nats.core.outbound.OutboundClassification;
import com.threeai.nats.core.outbound.OutboundClassificationProperties;

/**
 * F-7 (docs/12 D-D v2): the ENGINE-SCOPED read surface for outbound classification. Extends
 * the shared YAML-bound {@code spring.nats.outbound} properties so the existing
 * {@code NatsOutboundPublisher} wiring needs no signature change, but every read delegates to
 * the wrapped shared bean — overlay first, YAML base second. The overlay snapshot is an
 * immutable map swapped atomically by the reconciler each pass; readers (ExecutionListeners
 * on engine threads) are lock-free. Because the holder lives HERE — per engine module, not on
 * the shared properties bean — two engines in one Spring context cannot clobber each other's
 * overlays while the YAML base stays common.
 */
public class OverlayedOutboundClassification extends OutboundClassificationProperties {

    private final OutboundClassificationProperties base;
    private volatile Map<String, OutboundOverlayDefinition> overlay = Map.of();

    public OverlayedOutboundClassification(OutboundClassificationProperties base) {
        this.base = base;
    }

    /** Reconciler-only: atomically replaces the definition overlay (D-D: revert on removal). */
    public void swapOverlay(Map<String, OutboundOverlayDefinition> byMessageType) {
        this.overlay = Map.copyOf(byMessageType);
    }

    public Map<String, OutboundOverlayDefinition> currentOverlay() {
        return overlay;
    }

    @Override
    public OutboundClassification classify(String messageType) {
        OutboundOverlayDefinition definition = overlay.get(messageType);
        if (definition != null) {
            return definition.critical() ? OutboundClassification.CRITICAL
                    : OutboundClassification.BEST_EFFORT;
        }
        return base.classify(messageType);
    }

    @Override
    public List<String> variableAllowlistFor(String messageType) {
        OutboundOverlayDefinition definition = overlay.get(messageType);
        if (definition != null) {
            return definition.variableAllowlist();
        }
        return base.variableAllowlistFor(messageType);
    }

    // The remaining surface delegates verbatim — this class must be indistinguishable from
    // the base bean everywhere the overlay does not apply (subclass state is never used).

    @Override
    public boolean isEnabled() {
        return base.isEnabled();
    }

    @Override
    public void setEnabled(boolean enabled) {
        base.setEnabled(enabled);
    }

    @Override
    public Set<String> getCriticalTypes() {
        return base.getCriticalTypes();
    }

    @Override
    public void setCriticalTypes(Set<String> criticalTypes) {
        base.setCriticalTypes(criticalTypes);
    }

    @Override
    public Map<String, List<String>> getVariableAllowlist() {
        return base.getVariableAllowlist();
    }

    @Override
    public void setVariableAllowlist(Map<String, List<String>> variableAllowlist) {
        base.setVariableAllowlist(variableAllowlist);
    }
}
