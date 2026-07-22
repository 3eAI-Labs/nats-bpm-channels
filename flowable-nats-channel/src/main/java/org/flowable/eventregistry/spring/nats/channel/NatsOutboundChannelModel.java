package org.flowable.eventregistry.spring.nats.channel;

import org.flowable.eventregistry.model.OutboundChannelModel;

public class NatsOutboundChannelModel extends OutboundChannelModel {

    private String subject;
    private boolean jetstream;
    private boolean autoCreateStream;
    private String streamName;
    /** D-G' (docs/09-outbound-handoff.md) — DLQ subject for publish-failure custody-transfer;
     *  {@code null} means "use the default {@code dlq.<subject>} convention" (mirrors the inbound
     *  channel model's own default resolution in {@code NatsChannelDefinitionProcessor}). */
    private String dlqSubject;

    public String getSubject() {
        return subject;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }

    public boolean isJetstream() {
        return jetstream;
    }

    public void setJetstream(boolean jetstream) {
        this.jetstream = jetstream;
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

    public String getDlqSubject() {
        return dlqSubject;
    }

    public void setDlqSubject(String dlqSubject) {
        this.dlqSubject = dlqSubject;
    }
}
