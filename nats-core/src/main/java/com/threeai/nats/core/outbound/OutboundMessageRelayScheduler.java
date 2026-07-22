package com.threeai.nats.core.outbound;

import static net.logstash.logback.argument.StructuredArguments.kv;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;

/**
 * Drives {@link OutboundMessageRelay#relayCycle()} periodically — basamak-2 {@code
 * HistoryOutboxRelayScheduler} precedent (manual daemon {@link ScheduledExecutorService} rather
 * than relying on tenant-enabled {@code @EnableScheduling}, same rationale: {@code relayCycle()}
 * is safe to invoke more often than strictly necessary, leader-check + idempotent per-row logic).
 */
public class OutboundMessageRelayScheduler implements InitializingBean, DisposableBean {

    private static final Logger log = LoggerFactory.getLogger(OutboundMessageRelayScheduler.class);

    private final OutboundMessageRelay outboxRelay;
    private final long periodSeconds;
    private final String engineId;

    private ScheduledExecutorService scheduler;

    public OutboundMessageRelayScheduler(OutboundMessageRelay outboxRelay, long periodSeconds, String engineId) {
        this.outboxRelay = outboxRelay;
        this.periodSeconds = periodSeconds;
        this.engineId = engineId;
    }

    @Override
    public void afterPropertiesSet() {
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "outbound-outbox-relay-" + engineId);
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleWithFixedDelay(this::runCycleSafely, periodSeconds, periodSeconds, TimeUnit.SECONDS);
        log.info("Outbound-outbox relay scheduler started", kv("engine_id", engineId), kv("period_seconds", periodSeconds));
    }

    private void runCycleSafely() {
        try {
            outboxRelay.relayCycle();
        } catch (Exception e) {
            log.error("Uncaught exception in outbound-outbox relay cycle — will retry next cycle",
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
