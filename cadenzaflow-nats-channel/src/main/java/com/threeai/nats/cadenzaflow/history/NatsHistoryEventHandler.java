package com.threeai.nats.cadenzaflow.history;

import static net.logstash.logback.argument.StructuredArguments.kv;

import java.sql.Connection;
import java.util.List;

import com.threeai.nats.core.history.HistoryClassNames;
import org.cadenzaflow.bpm.engine.impl.cfg.TransactionState;
import org.cadenzaflow.bpm.engine.impl.context.Context;
import org.cadenzaflow.bpm.engine.impl.history.event.HistoryEvent;
import org.cadenzaflow.bpm.engine.impl.history.handler.DbHistoryEventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Composite, class-based router — the fork SPI entry point (BR-HDL-001/002/005/007, ADR-0009,
 * `03_classes/1_handler_outbox.md` §1). Registered into
 * {@code ProcessEngineConfigurationImpl.customHistoryEventHandlers};
 * {@code enableDefaultDbHistoryEventHandler} is ALWAYS set to {@code false} at bootstrap by the
 * module's auto-configuration — this class owns its own internal {@link DbHistoryEventHandler}
 * delegate instead (fork-evidence rationale: `01_overview.md` "Phase3'ün devrettiği doğrulamalar
 * #1", §1.4 below).
 *
 * <p><b>cadenzaflow-nats-channel targets {@code org.cadenzaflow.*}</b> — byte-mirror of
 * {@code camunda-nats-channel} (which targets {@code org.camunda.*}, module convention,
 * phase4-review F-001), ADR-0007.
 *
 * <h2>1.4 — why {@code internalDbDelegate} is held by US, not the fork's {@code CompositeDbHistoryEventHandler}</h2>
 * {@code ProcessEngineConfigurationImpl.init()} (hence {@code initHistoryEventHandler()}) runs
 * exactly ONCE at boot — the fork's {@code enableDefaultDbHistoryEventHandler} flag is
 * engine-wide and one-time, NOT per-class, so per-class cutover (BR-CUT-002) can only happen
 * INSIDE our own composite. {@link HistoryEventProcessorLiveReadNote} documents the runtime
 * behavior this design relies on.
 *
 * <p><b>CODER-NOTE (constructor signature, beyond the LLD sketch):</b> a trailing {@code String
 * engineId} parameter was added. The fork's {@code HistoryEventHandler.handleEvent(HistoryEvent)}
 * SPI method carries no engine identity of its own, yet both
 * {@code CompactHistoryOutboxWriter.write(...)} ({@code compact_history_outbox.engine_id NOT
 * NULL}) and {@code HistoryPostCommitPublisher.publish(HistoryEvent, String, String engineId)}
 * (LLD-specified signature) require it — so this class must hold it.
 */
public class NatsHistoryEventHandler implements org.cadenzaflow.bpm.engine.impl.history.handler.HistoryEventHandler {

    private static final Logger log = LoggerFactory.getLogger(NatsHistoryEventHandler.class);

    private final ClassCutoverStateRegistry cutoverRegistry;
    private final HistoryClassificationProperties classification;
    private final CompactHistoryOutboxWriter outboxWriter;
    private final HistoryPostCommitPublisher postCommitPublisher;
    private final DbHistoryEventHandler internalDbDelegate;
    private final String engineId;

    public NatsHistoryEventHandler(
            ClassCutoverStateRegistry cutoverRegistry,
            HistoryClassificationProperties classification,
            CompactHistoryOutboxWriter outboxWriter,
            HistoryPostCommitPublisher postCommitPublisher,
            DbHistoryEventHandler internalDbDelegate,
            String engineId) {
        this.cutoverRegistry = cutoverRegistry;
        this.classification = classification;
        this.outboxWriter = outboxWriter;
        this.postCommitPublisher = postCommitPublisher;
        this.internalDbDelegate = internalDbDelegate;
        this.engineId = engineId;
    }

    @Override
    public void handleEvent(HistoryEvent historyEvent) {
        String historyClass = HistoryEventClassResolver.resolve(historyEvent).orElse(null);
        if (historyClass == null) {
            // VAL_HISTORY_CLASS_UNCLASSIFIED -- fail-safe bulk + WARN (runtime detection, not
            // bootstrap; the engine's producible class set can change with an engine upgrade).
            log.warn("Unclassified ACT_HI event — routing fail-safe as bulk (never cut over)",
                    kv("event_type", historyEvent.getEventType()),
                    kv("java_class", historyEvent.getClass().getSimpleName()));
            historyClass = HistoryClassNames.UNCLASSIFIED;
        }

        if (classification.isAuditCritical(historyClass)) {
            outboxWriter.write(historyEvent, historyClass, engineId, currentTransactionConnection());
        } else {
            String publishClass = historyClass;
            Context.getCommandContext().getTransactionContext()
                    .addTransactionListener(TransactionState.COMMITTED,
                            commandContext -> postCommitPublisher.publish(historyEvent, publishClass, engineId));
        }

        // Dual-run (BR-HDL-005): while the class has not been cut over, the ACT_HI write still
        // happens too. NFR-P1 is achieved once isCutOver(...) flips to true (post rolling-restart).
        if (!cutoverRegistry.isCutOver(historyClass)) {
            internalDbDelegate.handleEvent(historyEvent);
        }
    }

    /**
     * Fork's own {@code CompositeHistoryEventHandler.handleEvents(...)} degrades to a for-loop
     * over {@code handleEvent(...)} (fork-verified, HLD §11 kalem 2) — this override exists only
     * to preserve the SPI contract, no batch-specific optimization.
     */
    @Override
    public void handleEvents(List<HistoryEvent> historyEvents) {
        for (HistoryEvent historyEvent : historyEvents) {
            handleEvent(historyEvent);
        }
    }

    /**
     * MyBatis' {@code SqlSession} wraps the engine transaction's live JDBC {@link Connection} —
     * {@code DbSqlSession.getSqlSession().getConnection()} is the SAME pattern the fork's own
     * {@code DbSqlSession} internals use (verified against the compiled 7.24.0 engine source,
     * {@code DbSqlSession.java:619}).
     */
    private Connection currentTransactionConnection() {
        try {
            return Context.getCommandContext().getDbSqlSession().getSqlSession().getConnection();
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Unable to obtain the current engine transaction's JDBC connection for compact-outbox write", e);
        }
    }

    /**
     * Documentation-only marker referenced by the class Javadoc — the fork's
     * {@code HistoryEventProcessor.processHistoryEvents(...)} reads
     * {@code Context.getProcessEngineConfiguration().getHistoryEventHandler()} LIVE on every
     * event (no cache), which is why {@link ClassCutoverStateRegistry}'s per-class routing table
     * can theoretically be upgraded to a live KV watch in a future basamak without any fork
     * change (v1 stays boot-read-only per ARCH-Q5).
     */
    private interface HistoryEventProcessorLiveReadNote {
    }
}
