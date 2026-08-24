package com.threeai.nats.cadenzaflow.config;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicReference;

import com.threeai.nats.cadenzaflow.eventing.DefinitionSubscriberFactory;
import com.threeai.nats.cadenzaflow.eventing.EventingDeployer;
import com.threeai.nats.cadenzaflow.eventing.EventingReconcileScheduler;
import com.threeai.nats.cadenzaflow.eventing.EventingReconciler;
import com.threeai.nats.cadenzaflow.eventing.EventingRegistry;
import com.threeai.nats.cadenzaflow.eventing.LocationDefinitionLoader;
import com.threeai.nats.cadenzaflow.eventing.OverlayedOutboundClassification;
import com.threeai.nats.core.dlq.DlqPublisher;
import com.threeai.nats.core.jetstream.JetStreamStreamManager;
import com.threeai.nats.core.metrics.NatsChannelMetrics;
import com.threeai.nats.core.outbound.OutboundClassificationProperties;

import io.nats.client.Connection;
import io.nats.client.JetStream;
import org.cadenzaflow.bpm.engine.RepositoryService;
import org.cadenzaflow.bpm.engine.RuntimeService;
import org.cadenzaflow.bpm.engine.impl.cfg.AbstractProcessEnginePlugin;
import org.cadenzaflow.bpm.engine.impl.cfg.ProcessEngineConfigurationImpl;
import org.cadenzaflow.bpm.engine.impl.cfg.ProcessEnginePlugin;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.core.io.ResourceLoader;
import org.springframework.core.io.support.ResourcePatternUtils;

/**
 * Event-Registry parity wiring (docs/12, 0.11.0). Gated hard on
 * {@code spring.nats.cadenzaflow.eventing.enabled=true} (D-F v2, default false): absent the gate,
 * NONE of these beans exist — deployer, reconciler and locations are completely inert.
 *
 * <p><b>Dependency-cycle note (why the nudge relay exists):</b> the {@link ProcessEnginePlugin}
 * must be constructible BEFORE the engine (the starter consumes plugins at engine build), but
 * the reconciler needs engine services ({@link RepositoryService}). Wiring the deployer's nudge
 * directly to the scheduler would close the cycle plugin→deployer→scheduler→reconciler→engine→
 * plugin. The relay breaks it: the deployer holds the relay from the start; the scheduler
 * registers itself into the relay when it comes up. A nudge fired before that registration is
 * dropped deliberately — the scheduler's boot pass covers everything that happened earlier.
 */
@AutoConfiguration(after = CadenzaFlowNatsAutoConfiguration.class)
@ConditionalOnClass(org.cadenzaflow.bpm.engine.ProcessEngine.class)
@ConditionalOnProperty(prefix = "spring.nats.cadenzaflow.eventing", name = "enabled", havingValue = "true")
@EnableConfigurationProperties(CadenzaFlowEventingProperties.class)
public class CadenzaFlowEventingAutoConfiguration {

    /** Mutable nudge target; see the class Javadoc's dependency-cycle note. */
    static class NudgeRelay implements Runnable {
        private final AtomicReference<Runnable> target = new AtomicReference<>();

        void register(Runnable actual) {
            target.set(actual);
        }

        @Override
        public void run() {
            Runnable actual = target.get();
            if (actual != null) {
                actual.run();
            }
        }
    }

    @Bean
    public NudgeRelay eventingNudgeRelay() {
        return new NudgeRelay();
    }

    @Bean
    public EventingDeployer eventingDeployer(NudgeRelay eventingNudgeRelay) {
        return new EventingDeployer(eventingNudgeRelay);
    }

    @Bean
    public ProcessEnginePlugin eventingProcessEnginePlugin(EventingDeployer eventingDeployer) {
        return new AbstractProcessEnginePlugin() {
            @Override
            public void preInit(ProcessEngineConfigurationImpl configuration) {
                if (configuration.getCustomPostDeployers() == null) {
                    configuration.setCustomPostDeployers(new ArrayList<>());
                }
                configuration.getCustomPostDeployers().add(eventingDeployer);
            }
        };
    }

    @Bean
    public DefinitionSubscriberFactory eventingSubscriberFactory(Connection connection,
            JetStream jetStream, JetStreamStreamManager streamManager, RuntimeService runtimeService,
            @Autowired(required = false) NatsChannelMetrics metrics, DlqPublisher dlqPublisher,
            @Autowired(required = false) com.threeai.nats.core.shard.ShardTopology shardTopology) {
        DefinitionSubscriberFactory factory = new DefinitionSubscriberFactory(connection, jetStream,
                streamManager, runtimeService, metrics, dlqPublisher);
        factory.setShardTopology(shardTopology); // null = unsharded (docs/13; fingerprint stays shard-blind)
        return factory;
    }

    @Bean(destroyMethod = "close")
    public EventingRegistry eventingRegistry(DefinitionSubscriberFactory eventingSubscriberFactory) {
        return new EventingRegistry(eventingSubscriberFactory);
    }

    /**
     * F-7: the engine-scoped classification read surface. {@code @Primary} so the existing
     * {@code NatsOutboundPublisher} wiring (which injects by the base type) reads through the
     * overlay; every non-overlay call delegates to the shared YAML-bound bean.
     */
    @Bean
    @Primary
    public OverlayedOutboundClassification cadenzaflowOutboundClassificationOverlay(
            OutboundClassificationProperties base) {
        return new OverlayedOutboundClassification(base);
    }

    @Bean
    public EventingReconciler eventingReconciler(RepositoryService repositoryService,
            RuntimeService runtimeService, EventingRegistry eventingRegistry,
            CadenzaFlowEventingProperties properties, ResourceLoader resourceLoader,
            OverlayedOutboundClassification overlay) {
        EventingReconciler.StaticDefinitions locations = LocationDefinitionLoader.load(
                ResourcePatternUtils.getResourcePatternResolver(resourceLoader),
                properties.getDefinitions().getLocations());
        return new EventingReconciler(repositoryService, runtimeService, eventingRegistry,
                locations, overlay::swapOverlay);
    }

    @Bean(initMethod = "start", destroyMethod = "close")
    public EventingReconcileScheduler eventingReconcileScheduler(EventingReconciler eventingReconciler,
            EventingRegistry eventingRegistry, CadenzaFlowEventingProperties properties,
            NudgeRelay eventingNudgeRelay) {
        EventingReconcileScheduler scheduler = new EventingReconcileScheduler(
                eventingReconciler, eventingRegistry, properties.getReconcilePeriodSeconds());
        eventingNudgeRelay.register(scheduler::nudge);
        return scheduler;
    }
}
