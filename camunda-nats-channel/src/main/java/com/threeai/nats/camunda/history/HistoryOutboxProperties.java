package com.threeai.nats.camunda.history;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * {@code spring.nats.camunda.history.outbox.*} — relay cycle period + stuck-row alarm multiplier
 * (LLD-Q5/BA-Q7, `08_config.md` §1). Basamak-1 `08_config.md` §4 "ADR-0004 eşikleri sabit"
 * pattern is DELIBERATELY not repeated here — the stuck-threshold multiplier is left tenant/bench
 * calibratable (PO-Q4 pattern), per {@code 01_overview.md} LLD-Q5.
 */
@ConfigurationProperties(prefix = "spring.nats.camunda.history.outbox")
public class HistoryOutboxProperties {

    /** relayCyclePeriod default 30s ("Phase3'ün devrettiği doğrulamalar #3" RTO derivation input). */
    private long relayCyclePeriodSeconds = 30;

    /** BA-Q7 default 5x (LLD-Q5) — SYS_OUTBOX_ROW_STUCK threshold = multiplier * relayCyclePeriodSeconds. */
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
