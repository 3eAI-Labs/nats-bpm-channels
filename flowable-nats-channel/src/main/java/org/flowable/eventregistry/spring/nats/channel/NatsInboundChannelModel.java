package org.flowable.eventregistry.spring.nats.channel;

import java.time.Duration;

import org.flowable.eventregistry.model.InboundChannelModel;

public class NatsInboundChannelModel extends InboundChannelModel {

    private String subject;
    private String queueGroup;
    private boolean jetstream;
    private String durableName;
    private String deliverPolicy = "all";
    private Duration ackWait = Duration.ofSeconds(30);
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

    public String getQueueGroup() {
        return queueGroup;
    }

    public void setQueueGroup(String queueGroup) {
        this.queueGroup = queueGroup;
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

    public String getDeliverPolicy() {
        return deliverPolicy;
    }

    public void setDeliverPolicy(String deliverPolicy) {
        this.deliverPolicy = deliverPolicy;
    }

    public Duration getAckWait() {
        return ackWait;
    }

    public void setAckWait(Duration ackWait) {
        this.ackWait = ackWait;
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
