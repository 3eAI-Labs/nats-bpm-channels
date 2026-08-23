package com.threeai.nats.cibseven.config;

import java.util.ArrayList;
import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * {@code spring.nats.cibseven.eventing.*} — the Event-Registry-parity gate (docs/12 D-F v2).
 * Default OFF per the house rule: with {@code enabled=false} (or unset) the deployer,
 * reconciler and locations source are never created — the capability is completely inert.
 */
@ConfigurationProperties(prefix = "spring.nats.cibseven.eventing")
public class CibSevenEventingProperties {

    /** Master gate. The capability activates only when set explicitly (house rule: opt-in). */
    private boolean enabled = false;

    /** Fixed-delay reconcile period in seconds (docs/12 D-B v2; Flowable precedent). */
    private long reconcilePeriodSeconds = 60;

    private final Definitions definitions = new Definitions();

    public static class Definitions {
        /**
         * Secondary definition source (F-4a): Spring resource patterns for {@code .event}/
         * {@code .channel} files, e.g. {@code classpath*:eventing/*.event}. Locations have no
         * tenant (registered under {@code null#key}); an engine deployment with the exact
         * same key wins with one WARN per key.
         */
        private List<String> locations = new ArrayList<>();

        public List<String> getLocations() {
            return locations;
        }

        public void setLocations(List<String> locations) {
            this.locations = locations;
        }
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public long getReconcilePeriodSeconds() {
        return reconcilePeriodSeconds;
    }

    public void setReconcilePeriodSeconds(long reconcilePeriodSeconds) {
        this.reconcilePeriodSeconds = reconcilePeriodSeconds;
    }

    public Definitions getDefinitions() {
        return definitions;
    }
}
