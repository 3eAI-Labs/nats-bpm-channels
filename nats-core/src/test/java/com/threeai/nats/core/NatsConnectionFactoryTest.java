package com.threeai.nats.core;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.nats.client.Connection;
import io.nats.client.ConnectionListener;
import io.nats.client.Consumer;
import io.nats.client.ErrorListener;
import io.nats.client.Options;
import io.nats.client.api.ServerInfo;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Real NATS (Testcontainers) for {@link NatsConnectionFactory#create} itself (proves the builder
 * wiring actually produces a working connection, not a re-implementation of it), plus direct
 * invocation of the REAL {@link ConnectionListener}/{@link ErrorListener} instances {@code
 * create()} installed (retrieved via {@link Connection#getOptions()}) for every switch/branch —
 * these only fire on genuine NATS protocol events in production, so synthesizing the trigger here
 * (rather than provoking real network chaos) is the practical way to exercise each branch of
 * production code without flaky, timing-dependent fault injection.
 */
@Testcontainers
class NatsConnectionFactoryTest {

    private static GenericContainer<?> natsContainer;

    private Connection connection;

    @BeforeAll
    static void startContainer() {
        natsContainer = new GenericContainer<>("nats:2.10-alpine").withExposedPorts(4222);
        natsContainer.start();
    }

    @AfterAll
    static void stopContainer() {
        natsContainer.stop();
    }

    @AfterEach
    void closeConnection() throws InterruptedException {
        if (connection != null) {
            connection.close();
        }
    }

    private NatsProperties propsForContainer() {
        NatsProperties props = new NatsProperties();
        props.setUrl("nats://" + natsContainer.getHost() + ":" + natsContainer.getMappedPort(4222));
        return props;
    }

    @Test
    void create_defaultAuth_connectsSuccessfully() throws Exception {
        connection = NatsConnectionFactory.create(propsForContainer());

        assertThatCode(() -> connection.flush(java.time.Duration.ofSeconds(2))).doesNotThrowAnyException();
    }

    @Test
    void create_usernamePasswordAuth_stillConnects_unauthenticatedServerIgnoresIt() throws Exception {
        NatsProperties props = propsForContainer();
        props.setUsername("svc-account");
        props.setPassword("s3cr3t");

        connection = NatsConnectionFactory.create(props);

        assertThatCode(() -> connection.flush(java.time.Duration.ofSeconds(2))).doesNotThrowAnyException();
    }

    @Test
    void create_tokenAuth_stillConnects_unauthenticatedServerIgnoresIt() throws Exception {
        NatsProperties props = propsForContainer();
        props.setToken("tok-123");

        connection = NatsConnectionFactory.create(props);

        assertThatCode(() -> connection.flush(java.time.Duration.ofSeconds(2))).doesNotThrowAnyException();
    }

    /**
     * {@code Nats.credentials(path)} lazily wraps the path into a {@code FileAuthHandler} — it is
     * never read unless the server actually challenges for nkey-signed auth, which an
     * auth-disabled server (this container) never does. Proves {@code configureAuth}'s
     * credentials-file branch is wired without requiring a real signed-creds fixture.
     */
    @Test
    void create_credentialsFileAuth_stillConnects_fileNeverReadByUnauthenticatedServer() throws Exception {
        NatsProperties props = propsForContainer();
        props.setCredentialsFile("/nonexistent/path.creds");

        connection = NatsConnectionFactory.create(props);

        assertThatCode(() -> connection.flush(java.time.Duration.ofSeconds(2))).doesNotThrowAnyException();
    }

    @Test
    void create_nkeyFileAuth_stillConnects_fileNeverReadByUnauthenticatedServer() throws Exception {
        NatsProperties props = propsForContainer();
        props.setNkeyFile("/nonexistent/path.nk");

        connection = NatsConnectionFactory.create(props);

        assertThatCode(() -> connection.flush(java.time.Duration.ofSeconds(2))).doesNotThrowAnyException();
    }

    @Test
    void connectionListener_everyEvent_logsWithoutThrowing() throws Exception {
        connection = NatsConnectionFactory.create(propsForContainer());
        Options opts = connection.getOptions();
        ConnectionListener listener = opts.getConnectionListener();
        ServerInfo serverInfo = mock(ServerInfo.class);
        when(serverInfo.getHost()).thenReturn("test-host");
        Connection connWithServerInfo = mock(Connection.class);
        when(connWithServerInfo.getServerInfo()).thenReturn(serverInfo);

        for (ConnectionListener.Events event : ConnectionListener.Events.values()) {
            assertThatCode(() -> listener.connectionEvent(connWithServerInfo, event))
                    .as("event %s must not throw", event)
                    .doesNotThrowAnyException();
        }
    }

    @Test
    void errorListener_everyCallback_logsWithoutThrowing() throws Exception {
        connection = NatsConnectionFactory.create(propsForContainer());
        Options opts = connection.getOptions();
        ErrorListener listener = opts.getErrorListener();
        Consumer consumer = mock(Consumer.class);
        when(consumer.getPendingMessageCount()).thenReturn(5L);

        assertThatCode(() -> listener.errorOccurred(connection, "simulated NATS error"))
                .doesNotThrowAnyException();
        assertThatCode(() -> listener.exceptionOccurred(connection, new java.io.IOException("simulated")))
                .doesNotThrowAnyException();
        assertThatCode(() -> listener.slowConsumerDetected(connection, consumer))
                .doesNotThrowAnyException();
    }
}
