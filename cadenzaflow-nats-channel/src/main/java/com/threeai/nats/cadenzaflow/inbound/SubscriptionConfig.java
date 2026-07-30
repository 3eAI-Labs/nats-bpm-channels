package com.threeai.nats.cadenzaflow.inbound;

public class SubscriptionConfig {

    private String subject;
    private String messageName;
    private String businessKeyHeader;
    private String businessKeyVariable;
    private boolean jetstream;
    private String durableName;
    private int maxDeliver = 5;
    private String dlqSubject;
    private boolean autoCreateStream;
    private String streamName;

    public String getSubject() {
        return subject;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }

    public String getMessageName() {
        return messageName;
    }

    public void setMessageName(String messageName) {
        this.messageName = messageName;
    }

    public String getBusinessKeyHeader() {
        return businessKeyHeader;
    }

    public void setBusinessKeyHeader(String businessKeyHeader) {
        this.businessKeyHeader = businessKeyHeader;
    }

    public String getBusinessKeyVariable() {
        return businessKeyVariable;
    }

    public void setBusinessKeyVariable(String businessKeyVariable) {
        this.businessKeyVariable = businessKeyVariable;
    }

    public boolean isJetstream() {
        return jetstream;
    }

    public void setJetstream(boolean jetstream) {
        this.jetstream = jetstream;
    }

    public String getDurableName() {
        return durableName;
    }

    public void setDurableName(String durableName) {
        this.durableName = durableName;
    }

    public int getMaxDeliver() {
        return maxDeliver;
    }

    public void setMaxDeliver(int maxDeliver) {
        this.maxDeliver = maxDeliver;
    }

    public String getDlqSubject() {
        return dlqSubject;
    }

    public void setDlqSubject(String dlqSubject) {
        this.dlqSubject = dlqSubject;
    }

    public boolean isAutoCreateStream() {
        return autoCreateStream;
    }

    public void setAutoCreateStream(boolean autoCreateStream) {
        this.autoCreateStream = autoCreateStream;
    }

    public String getStreamName() {
        return streamName;
    }

    public void setStreamName(String streamName) {
        this.streamName = streamName;
    }
}
