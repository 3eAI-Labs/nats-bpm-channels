package com.threeai.nats.core.outbound;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * {@code spring.nats.outbound.outbox.*} — relay cycle period + stuck-row alarm multiplier
 * (basamak-2 {@code HistoryOutboxProperties} precedent, docs/09-outbound-handoff.md D-F').
 */
@ConfigurationProperties(prefix = "spring.nats.outbound.outbox")
public class OutboundMessageOutboxProperties {

    private long relayCyclePeriodSeconds = 30;

    private int stuckThresholdMultiplier = 5;

    public long getRelayCyclePeriodSeconds() {
        return relayCyclePeriodSeconds;
    }

    public void setRelayCyclePeriodSeconds(long relayCyclePeriodSeconds) {
        this.relayCyclePeriodSeconds = relayCyclePeriodSeconds;
    }

    public int getStuckThresholdMultiplier() {
        return stuckThresholdMultiplier;
    }

    public void setStuckThresholdMultiplier(int stuckThresholdMultiplier) {
        this.stuckThresholdMultiplier = stuckThresholdMultiplier;
    }

    public long stuckThresholdSeconds() {
        return relayCyclePeriodSeconds * stuckThresholdMultiplier;
    }
}
