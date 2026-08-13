package com.threeai.nats.core;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "spring.nats")
public class NatsProperties {

    private String url = "nats://localhost:4222";
    private String username;
    private String password;
    private String token;
    private String credentialsFile;
    private String nkeyFile;
    private Duration connectionTimeout = Duration.ofSeconds(5);
    private int maxReconnects = -1;
    private Duration reconnectWait = Duration.ofSeconds(2);
    private final Tls tls = new Tls();
    private final Jetstream jetstream = new Jetstream();

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }

    public String getCredentialsFile() {
        return credentialsFile;
    }

    public void setCredentialsFile(String credentialsFile) {
        this.credentialsFile = credentialsFile;
    }

    public String getNkeyFile() {
        return nkeyFile;
    }

    public void setNkeyFile(String nkeyFile) {
        this.nkeyFile = nkeyFile;
    }

    public Duration getConnectionTimeout() {
        return connectionTimeout;
    }

    public void setConnectionTimeout(Duration connectionTimeout) {
        this.connectionTimeout = connectionTimeout;
    }

    public int getMaxReconnects() {
        return maxReconnects;
    }

    public void setMaxReconnects(int maxReconnects) {
        this.maxReconnects = maxReconnects;
    }

    public Duration getReconnectWait() {
        return reconnectWait;
    }

    public void setReconnectWait(Duration reconnectWait) {
        this.reconnectWait = reconnectWait;
    }

    public Tls getTls() {
        return tls;
    }

    public Jetstream getJetstream() {
        return jetstream;
    }

    public static class Jetstream {

        /**
         * Replica count for the KV buckets the library provisions (leader leases and cutover
         * state). The default matches the deployment LLD's literal, so a clustered NATS keeps
         * exactly the durability it had before this became configurable.
         *
         * <p>It has to be configurable because a single-node NATS server rejects any bucket with
         * more than one replica ({@code replicas > 1 not supported in non-clustered mode [10074]}),
         * which makes the engine unbootable — the value is not a property of the library but of
         * the broker it is pointed at. Set it to 1 for single-node development.
         *
         * <p>Applies to KV buckets only. JetStream streams are provisioned by the tenant.
         */
        private int kvReplicas = 3;

        public int getKvReplicas() {
            return kvReplicas;
        }

        public void setKvReplicas(int kvReplicas) {
            this.kvReplicas = kvReplicas;
        }

        /**
         * Replica count for streams created by the library's opt-in auto-create path
         * ({@code auto-create-stream: true} on a subscription or channel). Production streams are
         * provisioned by ops and are unaffected — see {@link
         * com.threeai.nats.core.jetstream.JetStreamStreamManager}.
         *
         * <p>Defaults to 1, which is what that path has always produced and what a single-node
         * dev server requires. The opposite default was considered and rejected: a clustered
         * default of 3 makes every single-node environment unbootable ({@code replicas > 1 not
         * supported in non-clustered mode [10074]}), which is exactly how {@link #kvReplicas}
         * behaved before it became configurable.
         *
         * <p>Leaving it at 1 against a clustered server is legitimate — a dev stream on a shared
         * cluster does not need quorum — but it is rarely what someone wants in production, and
         * a single-replica stream stops serving entirely when its one node is lost. That
         * combination is therefore logged as a warning rather than passed over in silence.
         */
        private int streamReplicas = 1;

        public int getStreamReplicas() {
            return streamReplicas;
        }

        public void setStreamReplicas(int streamReplicas) {
            this.streamReplicas = streamReplicas;
        }
    }

    public static class Tls {

        private boolean enabled = false;
        private String certFile;
        private String keyFile;
        private String caFile;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public String getCertFile() {
            return certFile;
        }

        public void setCertFile(String certFile) {
            this.certFile = certFile;
        }

        public String getKeyFile() {
            return keyFile;
        }

        public void setKeyFile(String keyFile) {
            this.keyFile = keyFile;
        }

        public String getCaFile() {
            return caFile;
        }

        public void setCaFile(String caFile) {
            this.caFile = caFile;
        }
    }
}
