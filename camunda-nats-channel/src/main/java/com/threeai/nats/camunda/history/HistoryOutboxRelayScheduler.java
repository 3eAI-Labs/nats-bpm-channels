package com.threeai.nats.camunda.history;

import static net.logstash.logback.argument.StructuredArguments.kv;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;

/**
 * Drives {@link HistoryOutboxRelay#relayCycle()} periodically. {@code HistoryOutboxRelay}'s
 * {@code relayCycle()} carries a {@code @Scheduled} annotation per the LLD sketch (documents the
 * intended period), but actual invocation here uses a manual daemon
 * {@link ScheduledExecutorService} — the SAME pattern basamak-1's {@code A2SubscriptionRegistrar}
 * uses for {@code A2OrphanSweep.sweepCycle()}. This avoids making {@code @EnableScheduling} an
 * implicit tenant-application obligation (Spring only processes {@code @Scheduled} methods when
 * scheduling infrastructure is explicitly enabled); {@code relayCycle()} is safe to invoke more
 * often than strictly necessary (leader-check + idempotent per-row logic), so running both
 * mechanisms in an app that DOES enable Spring scheduling would not be a correctness bug, only
 * redundant leader-check calls.
 */
public class HistoryOutboxRelayScheduler implements InitializingBean, DisposableBean {

    private static final Logger log = LoggerFactory.getLogger(HistoryOutboxRelayScheduler.class);

    private final HistoryOutboxRelay outboxRelay;
    private final long periodSeconds;
    private final String engineId;

    private ScheduledExecutorService scheduler;

    public HistoryOutboxRelayScheduler(HistoryOutboxRelay outboxRelay, long periodSeconds, String engineId) {
        this.outboxRelay = outboxRelay;
        this.periodSeconds = periodSeconds;
        this.engineId = engineId;
    }

    @Override
    public void afterPropertiesSet() {
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "history-outbox-relay-" + engineId);
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleWithFixedDelay(this::runCycleSafely, periodSeconds, periodSeconds, TimeUnit.SECONDS);
        log.info("History-outbox relay scheduler started", kv("engine_id", engineId), kv("period_seconds", periodSeconds));
    }

    private void runCycleSafely() {
        try {
            outboxRelay.relayCycle();
        } catch (Exception e) {
            log.error("Uncaught exception in history-outbox relay cycle — will retry next cycle",
                    kv("engine_id", engineId), e);
        }
    }

    @Override
    public void destroy() {
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
    }
}
