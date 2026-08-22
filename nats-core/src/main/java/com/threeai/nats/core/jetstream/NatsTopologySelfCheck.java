package com.threeai.nats.core.jetstream;

import java.util.List;

import io.nats.client.Connection;
import org.springframework.beans.factory.InitializingBean;

/**
 * Boot-time runner for {@link JetStreamTopologyCheck#inspectAndWarnSubjects}: one bean per engine
 * module, carrying the durable consumers that deployment will bind.
 *
 * <p><b>Ordering contract:</b> every registrar bean that subscribes durables takes this bean as a
 * constructor dependency purely for ORDER — the check's findings must print BEFORE the subscribe
 * attempt that would fail on them ([SUB-90012]), because that failure aborts the context startup
 * and would otherwise take the diagnostic down with it.
 *
 * <p>Reports; never repairs; never fails a boot (see {@link JetStreamTopologyCheck}).
 */
public final class NatsTopologySelfCheck implements InitializingBean {

    private final Connection connection;
    private final List<JetStreamTopologyCheck.SubjectBinding> bindings;
    private final int configuredKvReplicas;

    public NatsTopologySelfCheck(Connection connection,
            List<JetStreamTopologyCheck.SubjectBinding> bindings, int configuredKvReplicas) {
        this.connection = connection;
        this.bindings = List.copyOf(bindings);
        this.configuredKvReplicas = configuredKvReplicas;
    }

    @Override
    public void afterPropertiesSet() {
        try {
            JetStreamTopologyCheck.inspectAndWarnSubjects(connection, bindings, configuredKvReplicas);
        } catch (RuntimeException e) {
            // Last line of the never-fails-a-boot contract: whatever a broker, a proxy or a mock
            // hands back, a diagnostic that takes the context down is the defect class it exists
            // to prevent.
            org.slf4j.LoggerFactory.getLogger(NatsTopologySelfCheck.class)
                    .debug("Topology self-check skipped — unexpected failure", e);
        }
    }
}
