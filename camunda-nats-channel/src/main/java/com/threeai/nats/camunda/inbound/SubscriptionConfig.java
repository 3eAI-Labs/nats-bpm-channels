package com.threeai.nats.camunda.inbound;

public class SubscriptionConfig {

    private String subject;
    private String messageName;
    private String businessKeyHeader;
    private String businessKeyVariable;
    private boolean jetstream;
    private String durableName;
    /**
     * Queue group shared by every engine node subscribing to this subject. Correlation must happen
     * once per message: without a queue group a durable JetStream consumer is exclusive (only the
     * first node binds, the rest fail with {@code [SUB-90012]}), and a non-durable one — or a plain
     * core-NATS subscription — is private to each node, so every node correlates the same message
     * independently. Defaults to a name derived from {@code messageName}; set it explicitly only to
     * override that.
     */
    private String deliverGroup;
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

    public String getDeliverGroup() {
        return deliverGroup;
    }

    public void setDeliverGroup(String deliverGroup) {
        this.deliverGroup = deliverGroup;
    }

    /**
     * The configured queue group, or a stable default derived from the message name. Null only when
     * neither is available, in which case the subscription falls back to fan-out.
     */
    public String resolveDeliverGroup() {
        if (deliverGroup != null && !deliverGroup.isBlank()) {
            return deliverGroup;
        }
        return messageName != null && !messageName.isBlank() ? "correlation-" + messageName : null;
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
