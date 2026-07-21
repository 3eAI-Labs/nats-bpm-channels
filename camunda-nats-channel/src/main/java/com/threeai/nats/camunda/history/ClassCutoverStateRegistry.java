package com.threeai.nats.camunda.history;

import static net.logstash.logback.argument.StructuredArguments.kv;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import com.threeai.nats.core.history.HistoryClassNames;
import io.nats.client.Connection;
import io.nats.client.KeyValue;
import io.nats.client.api.KeyValueEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Engine-side READER of the {@code history-cutover-state} KV bucket (LLD-Q3,
 * `03_classes/4_cutover_reconciliation.md` §2.2). Read ONCE at engine bootstrap
 * ({@code ProcessEngineConfigurationImpl.init()} happens exactly once — fork-evidence,
 * `01_overview.md` "Phase3'ün devrettiği doğrulamalar #1") — NOT watched live in v1 (ARCH-Q5
 * rolling-restart is the locked apply mechanism). Consulted by {@link NatsHistoryEventHandler} on
 * every event with an O(1) map lookup, no I/O.
 */
public class ClassCutoverStateRegistry {

    private static final Logger log = LoggerFactory.getLogger(ClassCutoverStateRegistry.class);
    private static final String BUCKET = "history-cutover-state";

    private final Connection connection;
    private final String engineId;

    private volatile Map<String, Boolean> cutoverState = Map.of();

    public ClassCutoverStateRegistry(com.threeai.nats.core.jetstream.JetStreamKvManager kvManager,
            Connection connection, String engineId) {
        // kvManager kept for LLD signature parity — bucket provisioning happens once at bootstrap
        // via JetStreamKvManager#ensureBucket, invoked by the module's subscription registrar
        // (99_deployment.md §1 bootstrap order), not by this reader.
        this.connection = connection;
        this.engineId = engineId;
    }

    /**
     * Builds an immutable in-memory {@code Map<String,Boolean>} over the 15 known ACT_HI classes
     * ({@link HistoryClassNames#ALL_CLASSES}). Any read failure (missing bucket, KV entry absent,
     * I/O error) defaults that class to {@code false} (not cut over — fail-safe dual-run,
     * BR-CUT-002 "kapı açık kalır" spirit applied at boot time too).
     */
    public void loadAtBootstrap() {
        Map<String, Boolean> loaded = new HashMap<>();
        KeyValue keyValue;
        try {
            keyValue = connection.keyValue(BUCKET);
        } catch (IOException e) {
            log.warn("history-cutover-state KV bucket unavailable at bootstrap — "
                    + "all classes default to DUAL_RUN (not cut over)", kv("engine_id", engineId), e);
            HistoryClassNames.ALL_CLASSES.forEach(cls -> loaded.put(cls, false));
            this.cutoverState = Map.copyOf(loaded);
            return;
        }
        for (String historyClass : HistoryClassNames.ALL_CLASSES) {
            loaded.put(historyClass, readCutoverFlag(keyValue, historyClass));
        }
        this.cutoverState = Map.copyOf(loaded);
        log.info("Loaded history-cutover-state at bootstrap", kv("engine_id", engineId),
                kv("cut_over_classes", cutOverClassCount(loaded)));
    }

    private boolean readCutoverFlag(KeyValue keyValue, String historyClass) {
        String key = "cutover." + engineId + "." + historyClass;
        try {
            KeyValueEntry entry = keyValue.get(key);
            return entry != null && "true".equals(new String(entry.getValue(), StandardCharsets.UTF_8));
        } catch (Exception e) {
            log.warn("Failed to read history-cutover-state KV entry — defaulting class to DUAL_RUN",
                    kv("history_class", historyClass), kv("key", key), e);
            return false;
        }
    }

    private static long cutOverClassCount(Map<String, Boolean> loaded) {
        return loaded.values().stream().filter(Boolean::booleanValue).count();
    }

    /** O(1) lookup, no I/O — consulted on every {@code NatsHistoryEventHandler.handleEvent(...)}. */
    public boolean isCutOver(String historyClass) {
        return cutoverState.getOrDefault(historyClass, false);
    }
}
