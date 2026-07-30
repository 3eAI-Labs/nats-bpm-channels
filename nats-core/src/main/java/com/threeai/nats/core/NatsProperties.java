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
