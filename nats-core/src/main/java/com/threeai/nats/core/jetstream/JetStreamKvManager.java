package com.threeai.nats.core.jetstream;

import static net.logstash.logback.argument.StructuredArguments.kv;

import java.io.IOException;
import java.time.Duration;

import io.nats.client.Connection;
import io.nats.client.JetStreamApiException;
import io.nats.client.KeyValueManagement;
import io.nats.client.api.KeyValueConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * KV-bucket sibling of {@link JetStreamStreamManager} — mirrors its "check, then create on 404"
 * idempotent bootstrap pattern for JetStream KV buckets (ADR-0002, LLD 03_classes/1_nats_core_common.md §3.1).
 */
public class JetStreamKvManager {

    private static final Logger log = LoggerFactory.getLogger(JetStreamKvManager.class);

    public void ensureBucket(String bucketName, Duration ttl, int replicas, Connection connection) {
        try {
            KeyValueManagement kvm = connection.keyValueManagement();
            try {
                kvm.getBucketInfo(bucketName);
                log.debug("KV bucket exists", kv("bucket", bucketName));
            } catch (JetStreamApiException e) {
                if (e.getErrorCode() == 404) {
                    KeyValueConfiguration config = KeyValueConfiguration.builder()
                            .name(bucketName)
                            .ttl(ttl)
                            .replicas(replicas)
                            .build();
                    kvm.create(config);
                    log.info("KV bucket created", kv("bucket", bucketName), kv("ttl", ttl), kv("replicas", replicas));
                } else {
                    throw new IllegalStateException("Failed to check KV bucket '" + bucketName + "'", e);
                }
            }
        } catch (IOException e) {
            throw new IllegalStateException("I/O error while managing KV bucket '" + bucketName + "'", e);
        } catch (IllegalStateException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException("Unexpected error managing KV bucket '" + bucketName + "'", e);
        }
    }
}
