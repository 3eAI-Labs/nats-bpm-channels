package com.threeai.nats.camunda.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.ArrayList;

import javax.sql.DataSource;

import com.threeai.nats.camunda.a2.A2SubscriptionRegistrar;
import com.threeai.nats.camunda.history.HistoryOutboxRelay;
import com.threeai.nats.core.jetstream.JetStreamKvManager;
import com.threeai.nats.core.jetstream.SweepLeaderLease;
import io.nats.client.Connection;
import io.nats.client.JetStream;
import org.camunda.bpm.engine.ExternalTaskService;
import org.camunda.bpm.engine.ProcessEngine;
import org.camunda.bpm.engine.RuntimeService;
import org.camunda.bpm.engine.impl.cfg.ProcessEngineConfigurationImpl;
import org.camunda.bpm.engine.impl.cfg.ProcessEnginePlugin;
import org.camunda.bpm.engine.impl.history.HistoryLevelFull;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

class CamundaNatsAutoConfigurationTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(CamundaNatsAutoConfiguration.class))
            .withBean(Connection.class, () -> mock(Connection.class))
            .withBean(JetStream.class, () -> mock(JetStream.class))
            .withBean(RuntimeService.class, () -> mock(RuntimeService.class))
            .withBean(ExternalTaskService.class, () -> mock(ExternalTaskService.class))
            .withBean(ProcessEngine.class, () -> mock(ProcessEngine.class));

    /**
     * The context as a real host presents it. Every Camunda engine has a {@link DataSource} —
     * it is not an optional extra a deployment might omit — yet the runner above has none, so the
     * whole {@code @ConditionalOnBean(DataSource.class)} half of this auto-configuration (history
     * outbox, relay leases, relay scheduler, engine plugin, outbound relay) is never instantiated
     * by it. Anything that only goes wrong when those beans exist is therefore invisible to a
     * DataSource-free test, however many assertions it makes.
     *
     * <p>{@code JetStreamKvManager} is mocked because the DataSource-gated beans call
     * {@code ensureBucket(...)} while being created, which would drive a mocked
     * {@link Connection} into real KV calls. That is a separate concern from the wiring this
     * runner exists to exercise.
     */
    private final ApplicationContextRunner engineLikeRunner = runner
            .withBean(DataSource.class, () -> mock(DataSource.class))
            .withBean(JetStreamKvManager.class, () -> mock(JetStreamKvManager.class));

    /**
     * The offload capabilities are opt-in: nothing activates until it is switched on. Tests that
     * exercise those beans have to say so, exactly as a deployment does — which is the point of
     * the default, and why {@link #offloadDisabledByDefault_onlyMessagingActive()} exists.
     */
    private final ApplicationContextRunner offloadRunner = engineLikeRunner.withPropertyValues(
            "spring.nats.camunda.history.enabled=true",
            "spring.nats.outbound.enabled=true");

    @Test
    void autoConfiguration_registersSubscriptionRegistrar() {
        runner.run(context -> {
            assertThat(context).hasSingleBean(NatsSubscriptionRegistrar.class);
        });
    }

    @Test
    void autoConfiguration_registersDlqPublisher() {
        runner.run(context -> {
            assertThat(context).hasSingleBean(com.threeai.nats.core.dlq.DlqPublisher.class);
        });
    }

    @Test
    void autoConfiguration_registersTransportSecurityGuard() {
        runner.run(context -> {
            assertThat(context).hasSingleBean(com.threeai.nats.core.config.NatsTransportSecurityGuard.class);
        });
    }

    @Test
    void autoConfiguration_registersStreamManager() {
        runner.run(context -> {
            assertThat(context).hasSingleBean(com.threeai.nats.core.jetstream.JetStreamStreamManager.class);
        });
    }

    /**
     * The baseline every other assertion here depends on: with an engine DataSource present, the
     * context must actually start. If it cannot, the library cannot be put on the classpath of any
     * real engine, and no amount of DataSource-free green tests says otherwise.
     */
    @Test
    void autoConfiguration_withEngineDataSource_contextStarts() {
        engineLikeRunner.run(context -> assertThat(context).hasNotFailed());
    }

    /**
     * The plugin must survive the lifecycle the engine actually drives. In the Camunda 7 lineage
     * {@code initHistoryLevel()} runs inside {@code init()}, i.e. AFTER every plugin's
     * {@code preInit(...)}, so {@code configuration.getHistoryLevel()} is null for the whole of
     * {@code preInit} — unconditionally, for every deployment. {@code HistoryBootstrapValidatorTest}
     * cannot see this because it hands the validator a resolved history level, which is precisely
     * what the real caller does not have.
     */
    @Test
    void historyProcessEnginePlugin_preInit_toleratesUnresolvedHistoryLevel() {
        offloadRunner.run(context -> {
            assertThat(context).hasNotFailed();
            ProcessEnginePlugin plugin = context.getBean("historyProcessEnginePlugin", ProcessEnginePlugin.class);

            ProcessEngineConfigurationImpl configuration = mock(ProcessEngineConfigurationImpl.class);
            when(configuration.getHistoryLevel()).thenReturn(null); // engine state during preInit
            when(configuration.getCustomHistoryEventHandlers()).thenReturn(new ArrayList<>());

            assertThatCode(() -> plugin.preInit(configuration)).doesNotThrowAnyException();
        });
    }

    /**
     * ...and the validation itself must still happen, once the level is resolved. Moving the check
     * to {@code postInit} only counts as a fix if the check survives the move.
     */
    @Test
    void historyProcessEnginePlugin_postInit_runsValidationWithResolvedHistoryLevel() {
        offloadRunner.run(context -> {
            ProcessEnginePlugin plugin = context.getBean("historyProcessEnginePlugin", ProcessEnginePlugin.class);

            ProcessEngineConfigurationImpl configuration = mock(ProcessEngineConfigurationImpl.class);
            when(configuration.getHistoryLevel()).thenReturn(new HistoryLevelFull());

            assertThatCode(() -> plugin.postInit(configuration)).doesNotThrowAnyException();
            verify(configuration, atLeastOnce()).getHistoryLevel();
        });
    }

    /**
     * A single-node NATS server rejects any KV bucket with more than one replica
     * ({@code [10074]}), so the replica count is a property of the broker, not of this library —
     * and it has to reach {@code ensureBucket} from configuration. It used to be a literal 3 at
     * every call site, which made single-node NATS unusable with no way to say otherwise.
     */
    @Test
    void kvReplicas_configuredValueReachesEveryBucket() {
        JetStreamKvManager kvManager = mock(JetStreamKvManager.class);
        runner.withBean(DataSource.class, () -> mock(DataSource.class))
                .withBean(JetStreamKvManager.class, () -> kvManager)
                .withPropertyValues("spring.nats.jetstream.kv-replicas=1",
                        "spring.nats.camunda.history.enabled=true",
                        "spring.nats.outbound.enabled=true")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    verify(kvManager, atLeastOnce())
                            .ensureBucket(anyString(), any(Duration.class), eq(1), any(Connection.class));
                    verify(kvManager, never())
                            .ensureBucket(anyString(), any(Duration.class), eq(3), any(Connection.class));
                });
    }

    /**
     * Two {@code SweepLeaderLease} beans exist once a DataSource is present
     * ({@code historyRelayLeaderLease}, {@code outboundRelayLeaderLease}), and the injection points
     * that consume one of them select by parameter name. Parameter names only survive compilation
     * with {@code -parameters}, so this asserts the wiring resolves rather than falling back to a
     * by-type match that cannot be satisfied.
     */
    @Test
    void autoConfiguration_withEngineDataSource_bothLeaderLeasesResolvable() {
        offloadRunner.run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).getBeans(SweepLeaderLease.class)
                    .containsKeys("historyRelayLeaderLease", "outboundRelayLeaderLease");
            assertThat(context).hasSingleBean(HistoryOutboxRelay.class);
        });
    }

    /**
     * History offload must be switchable on its own. It used to be gated only on a DataSource
     * bean, and every engine has one, so adding this library for A2 alone also started publishing
     * every history event to NATS — which then needs the HISTORY / DLQ_HISTORY streams and the
     * compact_history_outbox table to exist, or every publish fails. With the switch off the
     * engine keeps its own default DB history handler and ACT_HI_* behaves as if this library
     * were not on the classpath.
     */
    @Test
    void historyDisabled_offloadBeansAbsent_contextStillStarts() {
        engineLikeRunner.withPropertyValues("spring.nats.camunda.history.enabled=false")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).doesNotHaveBean("historyProcessEnginePlugin");
                    assertThat(context).doesNotHaveBean("historyOutboxRelay");
                    assertThat(context).doesNotHaveBean("historyOutboxRelayScheduler");
                    assertThat(context).doesNotHaveBean("classCutoverStateRegistry");
                    assertThat(context).doesNotHaveBean("compactHistoryOutboxWriter");
                    assertThat(context).doesNotHaveBean("historyRelayLeaderLease");
                    // A2 is a separate capability and must survive history being switched off.
                    assertThat(context).hasSingleBean(A2SubscriptionRegistrar.class);
                });
    }

    /**
     * The default the datasheet's "independent and opt-in" depends on. A DataSource is the only
     * thing these beans used to need, and every engine has one — so simply having the library on
     * the classpath started history offload and the outbound relay, and the phrase was untrue.
     * Nothing offload-related may appear here.
     */
    @Test
    void offloadDisabledByDefault_onlyMessagingActive() {
        engineLikeRunner.run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).doesNotHaveBean("historyProcessEnginePlugin");
            assertThat(context).doesNotHaveBean("historyOutboxRelay");
            assertThat(context).doesNotHaveBean("historyRelayLeaderLease");
            assertThat(context).doesNotHaveBean("outboundMessageRelay");
            assertThat(context).doesNotHaveBean("outboundRelayLeaderLease");
            // Messaging is what you get for free; that must still be wired.
            assertThat(context).hasSingleBean(NatsSubscriptionRegistrar.class);
            assertThat(context).hasSingleBean(A2SubscriptionRegistrar.class);
        });
    }

    @Test
    void historyExplicitlyEnabled_offloadBeansPresent() {
        offloadRunner.run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).hasBean("historyProcessEnginePlugin");
            assertThat(context).hasBean("historyOutboxRelay");
        });
    }

    /**
     * The fourth capability needs its own switch for the same reason history did: it was gated on
     * a DataSource bean alone, so it provisioned its KV leader bucket and started its relay on
     * every boot of every engine that merely had this library on the classpath.
     */
    @Test
    void outboundDisabled_relayBeansAbsent_contextStillStarts() {
        engineLikeRunner.withPropertyValues("spring.nats.outbound.enabled=false")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).doesNotHaveBean("outboundMessageRelay");
                    assertThat(context).doesNotHaveBean("outboundRelayLeaderLease");
                    assertThat(context).doesNotHaveBean("natsOutboundPublisher");
                    assertThat(context).hasSingleBean(A2SubscriptionRegistrar.class);
                });
    }

    /**
     * All four offload capabilities off at once: the datasheet's "every capability is independent
     * and opt-in" only means something if the context still starts with none of them active.
     */
    @Test
    void allOffloadCapabilitiesDisabled_contextStillStarts() {
        engineLikeRunner.withPropertyValues(
                        "spring.nats.camunda.history.enabled=false",
                        "spring.nats.outbound.enabled=false",
                        "history.large-variable.enabled=false")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).doesNotHaveBean("historyProcessEnginePlugin");
                    assertThat(context).doesNotHaveBean("outboundMessageRelay");
                    assertThat(context).hasSingleBean(A2SubscriptionRegistrar.class);
                });
    }
}
