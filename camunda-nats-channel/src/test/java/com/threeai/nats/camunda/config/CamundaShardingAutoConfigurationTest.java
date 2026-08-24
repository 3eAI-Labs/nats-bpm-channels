package com.threeai.nats.camunda.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.List;

import com.threeai.nats.core.dlq.DlqPublisher;
import com.threeai.nats.core.shard.ShardBootstrapValidator;
import com.threeai.nats.core.shard.ShardRouter;
import com.threeai.nats.core.shard.ShardTopology;

import com.threeai.nats.camunda.a2.A2Properties;

import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamManagement;
import io.nats.client.api.DiscardPolicy;
import io.nats.client.api.RetentionPolicy;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.api.StreamInfo;
import org.camunda.bpm.engine.ProcessEngine;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

/** docs/13 D-F: gate defaults CLOSED; open, the chain wires and the validator gates boot. */
class CamundaShardingAutoConfigurationTest {

    private Connection validTopologyConnection() throws Exception {
        Connection connection = mock(Connection.class);
        JetStreamManagement jsm = mock(JetStreamManagement.class);
        when(connection.jetStreamManagement()).thenReturn(jsm);
        when(jsm.getStreamNames(anyString())).thenAnswer(inv -> {
            String filter = inv.getArgument(0);
            if (filter.startsWith("shard.0.")) {
                return List.of("SHARD-S0");
            }
            if (filter.startsWith("shard.1.")) {
                return List.of("SHARD-S1");
            }
            return List.of(); // no legacy reply streams
        });
        StreamConfiguration config = StreamConfiguration.builder()
                .name("SHARD-SX").subjects("shard.x.>")
                .retentionPolicy(RetentionPolicy.WorkQueue)
                .discardPolicy(DiscardPolicy.New)
                .maxBytes(1024 * 1024)
                .duplicateWindow(Duration.ofSeconds(10_000)) // above any horizon here
                .build();
        StreamInfo info = mock(StreamInfo.class);
        when(info.getConfiguration()).thenReturn(config);
        when(jsm.getStreamInfo(anyString())).thenReturn(info);
        return connection;
    }

    private ApplicationContextRunner runner(Connection connection) {
        return new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(CamundaShardingAutoConfiguration.class))
                .withBean(Connection.class, () -> connection)
                .withBean(JetStream.class, () -> mock(JetStream.class))
                .withBean(ProcessEngine.class, () -> mock(ProcessEngine.class))
                .withBean(DlqPublisher.class, () -> mock(DlqPublisher.class))
                .withBean(A2Properties.class, A2Properties::new);
    }

    @Test
    void gateClosedByDefault_nothingShardShapedExists() {
        runner(mock(Connection.class)).run(context -> {
            assertThat(context).doesNotHaveBean(ShardTopology.class);
            assertThat(context).doesNotHaveBean(ShardRouter.class);
            assertThat(context).doesNotHaveBean(ShardBootstrapValidator.class);
            assertThat(context).doesNotHaveBean("shardBirthGuardPlugin");
        });
    }

    @Test
    void gateOpen_validTopology_wiresTopologyRouterGuardPlugin() throws Exception {
        runner(validTopologyConnection())
                .withPropertyValues("spring.nats.camunda.sharding.enabled=true",
                        "spring.nats.camunda.sharding.shard-count=2",
                        "spring.nats.camunda.sharding.shard-id=1",
                        "spring.nats.camunda.sharding.routes[0].subject=evt.order.accept",
                        "spring.nats.camunda.sharding.routes[0].business-key-field=orderId")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context.getBean(ShardTopology.class).getShardId()).isEqualTo(1);
                    assertThat(context.getBean(ShardTopology.class).getShardCount()).isEqualTo(2);
                    assertThat(context).hasSingleBean(ShardRouter.class);
                    assertThat(context).hasBean("shardBirthGuardPlugin");
                });
    }

    @Test
    void gateOpen_validatorGatesBoot_missingStreamFailsContext() {
        Connection connection = mock(Connection.class);
        try {
            JetStreamManagement jsm = mock(JetStreamManagement.class);
            when(connection.jetStreamManagement()).thenReturn(jsm);
            when(jsm.getStreamNames(anyString())).thenReturn(List.of()); // nothing provisioned
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        runner(connection)
                .withPropertyValues("spring.nats.camunda.sharding.enabled=true",
                        "spring.nats.camunda.sharding.shard-count=2",
                        "spring.nats.camunda.sharding.shard-id=0")
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure())
                            .hasStackTraceContaining("SHARD-BOOT-STREAM-MISSING");
                });
    }

    @Test
    void gateOpen_invalidShardId_failsWithConfigCode() throws Exception {
        runner(validTopologyConnection())
                .withPropertyValues("spring.nats.camunda.sharding.enabled=true",
                        "spring.nats.camunda.sharding.shard-count=2",
                        "spring.nats.camunda.sharding.shard-id=2")
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure())
                            .hasStackTraceContaining("VAL_SHARD_CONFIG");
                });
    }
}
