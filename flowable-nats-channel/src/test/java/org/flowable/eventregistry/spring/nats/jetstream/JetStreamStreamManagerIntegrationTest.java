package org.flowable.eventregistry.spring.nats.jetstream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.threeai.nats.core.jetstream.JetStreamStreamManager;
import io.nats.client.Connection;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamManagement;
import io.nats.client.Nats;
import io.nats.client.api.StreamInfo;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
class JetStreamStreamManagerIntegrationTest {

    @Container
    static GenericContainer<?> natsContainer = new GenericContainer<>("nats:2.10-alpine")
            .withCommand("--jetstream")
            .withExposedPorts(4222);

    private Connection connection;
    private JetStreamManagement jsm;

    @BeforeEach
    void setUp() throws Exception {
        String url = "nats://" + natsContainer.getHost() + ":" + natsContainer.getMappedPort(4222);
        connection = Nats.connect(url);
        jsm = connection.jetStreamManagement();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    void autoCreateStream_createsAndVerifies() throws Exception {
        // Verify stream does not exist
        assertThatThrownBy(() -> jsm.getStreamInfo("AUTO-TEST"))
                .isInstanceOf(JetStreamApiException.class)
                .satisfies(ex -> assertThat(((JetStreamApiException) ex).getErrorCode()).isEqualTo(404));

        // Call ensureStream
        JetStreamStreamManager streamManager = new JetStreamStreamManager();
        streamManager.ensureStream("AUTO-TEST", "auto.test", connection);

        // Verify stream now exists
        StreamInfo info = jsm.getStreamInfo("AUTO-TEST");
        assertThat(info).isNotNull();
        assertThat(info.getConfiguration().getName()).isEqualTo("AUTO-TEST");
        assertThat(info.getConfiguration().getSubjects()).contains("auto.test");
    }
}
