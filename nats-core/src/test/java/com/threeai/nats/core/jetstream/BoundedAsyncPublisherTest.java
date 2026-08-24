package com.threeai.nats.core.jetstream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import io.nats.client.JetStream;
import io.nats.client.api.PublishAck;
import io.nats.client.impl.NatsMessage;
import org.junit.jupiter.api.Test;

/** G4-P async publish: callbacks, bounded in-flight blocking, quiescence. */
class BoundedAsyncPublisherTest {

    private NatsMessage msg() {
        return NatsMessage.builder().subject("jobs.t").data(new byte[0]).build();
    }

    @Test
    void ackCompletes_successCallbackFires() throws Exception {
        JetStream js = mock(JetStream.class);
        CompletableFuture<PublishAck> future = new CompletableFuture<>();
        when(js.publishAsync(any(NatsMessage.class))).thenReturn(future);
        BoundedAsyncPublisher publisher = new BoundedAsyncPublisher(js, 4);
        CountDownLatch ok = new CountDownLatch(1);

        publisher.publish(msg(), ok::countDown, e -> { });
        future.complete(null);

        assertThat(ok.await(2, TimeUnit.SECONDS)).isTrue();
        assertThat(publisher.awaitQuiescence(1000)).isTrue();
    }

    @Test
    void ackFails_failureCallbackFires_sameSurfaceAsSync() throws Exception {
        JetStream js = mock(JetStream.class);
        CompletableFuture<PublishAck> future = new CompletableFuture<>();
        when(js.publishAsync(any(NatsMessage.class))).thenReturn(future);
        BoundedAsyncPublisher publisher = new BoundedAsyncPublisher(js, 4);
        AtomicReference<Throwable> seen = new AtomicReference<>();
        CountDownLatch failed = new CountDownLatch(1);

        publisher.publish(msg(), () -> { }, e -> { seen.set(e); failed.countDown(); });
        future.completeExceptionally(new IOException("Timeout or no response"));

        assertThat(failed.await(2, TimeUnit.SECONDS)).isTrue();
        assertThat(seen.get()).hasMessageContaining("Timeout");
    }

    @Test
    void inFlightCap_blocksCaller_untilAnAckFrees() throws Exception {
        JetStream js = mock(JetStream.class);
        CompletableFuture<PublishAck> f1 = new CompletableFuture<>();
        CompletableFuture<PublishAck> f2 = new CompletableFuture<>();
        AtomicInteger calls = new AtomicInteger();
        when(js.publishAsync(any(NatsMessage.class)))
                .thenAnswer(inv -> calls.incrementAndGet() == 1 ? f1 : f2);
        BoundedAsyncPublisher publisher = new BoundedAsyncPublisher(js, 1);

        publisher.publish(msg(), () -> { }, e -> { }); // doldurur
        CountDownLatch secondEntered = new CountDownLatch(1);
        Thread t = new Thread(() -> {
            publisher.publish(msg(), () -> { }, e -> { }); // BLOKLANMALI
            secondEntered.countDown();
        });
        t.start();

        assertThat(secondEntered.await(300, TimeUnit.MILLISECONDS))
                .as("cap doluyken ikinci publish gecmemeli").isFalse();
        f1.complete(null); // bir ACK slotu acar
        assertThat(secondEntered.await(2, TimeUnit.SECONDS)).isTrue();
        f2.complete(null);
        t.join(2000);
    }

    @Test
    void publishAsyncThrowsSynchronously_permitReleased_failureFires() {
        JetStream js = mock(JetStream.class);
        when(js.publishAsync(any(NatsMessage.class))).thenThrow(new IllegalStateException("closed"));
        BoundedAsyncPublisher publisher = new BoundedAsyncPublisher(js, 1);
        AtomicInteger failures = new AtomicInteger();

        publisher.publish(msg(), () -> { }, e -> failures.incrementAndGet());
        publisher.publish(msg(), () -> { }, e -> failures.incrementAndGet()); // slot sizmadi

        assertThat(failures.get()).isEqualTo(2);
        assertThat(publisher.awaitQuiescence(500)).isTrue();
    }
}
