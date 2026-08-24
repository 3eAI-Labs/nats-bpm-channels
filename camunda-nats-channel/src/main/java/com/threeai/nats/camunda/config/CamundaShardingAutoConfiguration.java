package com.threeai.nats.camunda.config;

import java.util.List;

import com.threeai.nats.camunda.a2.A2Properties;
import com.threeai.nats.camunda.eventing.EventingPayloadMapper;
import com.threeai.nats.camunda.shard.ShardBirthGuardParseListener;
import com.threeai.nats.core.dlq.DlqPublisher;
import com.threeai.nats.core.metrics.NatsChannelMetrics;
import com.threeai.nats.core.shard.PayloadFieldReader;
import com.threeai.nats.core.shard.ShardBootstrapValidator;
import com.threeai.nats.core.shard.ShardRouteConfig;
import com.threeai.nats.core.shard.ShardRouter;
import com.threeai.nats.core.shard.ShardTopology;

import io.nats.client.Connection;
import io.nats.client.JetStream;
import org.camunda.bpm.engine.impl.cfg.AbstractProcessEnginePlugin;
import org.camunda.bpm.engine.impl.cfg.ProcessEngineConfigurationImpl;
import org.camunda.bpm.engine.impl.cfg.ProcessEnginePlugin;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

/**
 * Instance-sharding wiring (docs/13, 0.12.0). Gated hard on
 * {@code spring.nats.camunda.sharding.enabled=true}: absent the gate NONE of these beans
 * exist. Declared {@code before} the main auto-configuration so the {@link ShardTopology}
 * bean definition is registered before the beans that optionally consume it are created —
 * the main configuration's registrars take it as an optional dependency and rewrite their
 * subscriptions when present.
 *
 * <p>Boot order: {@link ShardBootstrapValidator#validate()} runs as this configuration's
 * hard gate (initMethod, THROWS — docs/13 F-7/T-7); the router and every registrar bean
 * that binds subscriptions takes the validator as an ordering-only dependency, so nothing
 * subscribes before the topology is proven sane.
 */
@AutoConfiguration(before = CamundaNatsAutoConfiguration.class)
@ConditionalOnClass(org.camunda.bpm.engine.ProcessEngine.class)
@ConditionalOnProperty(prefix = "spring.nats.camunda.sharding", name = "enabled", havingValue = "true")
@EnableConfigurationProperties(CamundaShardingProperties.class)
public class CamundaShardingAutoConfiguration {

    @Bean
    public ShardTopology shardTopology(CamundaShardingProperties properties) {
        return new ShardTopology(properties.getShardCount(), properties.getShardId());
    }

    @Bean(initMethod = "validate")
    public ShardBootstrapValidator shardBootstrapValidator(Connection connection,
            ShardTopology topology, CamundaShardingProperties properties,
            A2Properties a2Properties) {
        // T-8: the duplicate-window invariant's right-hand side — max redelivery horizon
        // over the router consumer and the A2 completion consumers. Correlation subscribers
        // hardcode ackWait 30s / maxDeliver+1, covered by the same max.
        long routerHorizon = properties.getRouterAckWaitSeconds()
                * (properties.getRouterMaxDeliver() + 1L);
        long a2Horizon = a2Properties.getDefaults().getAckWaitSeconds()
                * (a2Properties.getDefaults().getMaxDeliver() + 1L);
        long correlationHorizon = 30L * 6L;
        long horizon = Math.max(routerHorizon, Math.max(a2Horizon, correlationHorizon));
        return new ShardBootstrapValidator(connection, topology, horizon);
    }

    /** Jackson-backed top-level reader — the seam nats-core deliberately does not implement. */
    @Bean
    public PayloadFieldReader shardPayloadFieldReader() {
        return EventingPayloadMapper::topLevelScalarAsText;
    }

    @Bean(initMethod = "subscribe", destroyMethod = "unsubscribe")
    public ShardRouter shardRouter(Connection connection, JetStream jetStream,
            ShardTopology topology, CamundaShardingProperties properties,
            PayloadFieldReader payloadFieldReader, DlqPublisher dlqPublisher,
            @Autowired(required = false) NatsChannelMetrics metrics,
            ShardBootstrapValidator validator) { // ordering only: validate before binding
        List<ShardRouteConfig> routes = properties.getRoutes().stream()
                .map(r -> new ShardRouteConfig(r.getSubject(), r.getBusinessKeyField()))
                .toList();
        return new ShardRouter(connection, jetStream, topology, routes, payloadFieldReader,
                dlqPublisher, metrics, properties.getRouterAckWaitSeconds());
    }

    @Bean
    public ProcessEnginePlugin shardBirthGuardPlugin(ShardTopology topology) {
        return new AbstractProcessEnginePlugin() {
            @Override
            public void preInit(ProcessEngineConfigurationImpl configuration) {
                if (configuration.getCustomPreBPMNParseListeners() == null) {
                    configuration.setCustomPreBPMNParseListeners(new java.util.ArrayList<>());
                }
                configuration.getCustomPreBPMNParseListeners()
                        .add(new ShardBirthGuardParseListener(topology));
            }
        };
    }
}
