package com.threeai.nats.camunda.eventing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.threeai.nats.core.exception.TopicNamespaceCollisionException;
import org.junit.jupiter.api.Test;

/** docs/12 D-A v2: strict subset parsing — rejection over silent degradation. */
class EventingDefinitionParserTest {

    private static final String CHANNEL_OK = """
            { "key": "orderChannel", "channelType": "inbound", "type": "nats",
              "deserializerType": "json",
              "channelEventKeyDetection": { "fixedValue": "orderEvent" },
              "queueGroup": "eventing-orders",
              "subject": "order.new" }
            """;

    @Test
    void event_fullSubset_parses_withExtensionMessageName() {
        EventDefinition def = EventingDefinitionParser.parseEvent("order.event", """
                { "key": "orderEvent", "name": "Order Event",
                  "correlationParameters": [ { "name": "orderId", "type": "string" } ],
                  "payload": [ { "name": "orderId", "type": "string" },
                               { "name": "amount", "type": "long" } ],
                  "extension": { "messageName": "OrderMessage" } }
                """);
        assertThat(def.key()).isEqualTo("orderEvent");
        assertThat(def.correlationParameterTypes()).containsEntry("orderId", "string");
        assertThat(def.payloadTypes()).containsEntry("amount", "long");
        assertThat(def.resolvedMessageName()).isEqualTo("OrderMessage");
    }

    @Test
    void event_withoutExtension_messageNameDefaultsToKey() {
        EventDefinition def = EventingDefinitionParser.parseEvent("e", "{ \"key\": \"orderEvent\" }");
        assertThat(def.resolvedMessageName()).isEqualTo("orderEvent");
    }

    @Test
    void event_typeOutsideDictionary_rejected() {
        assertThatThrownBy(() -> EventingDefinitionParser.parseEvent("e", """
                { "key": "k", "payload": [ { "name": "d", "type": "Date" } ] }
                """))
                .isInstanceOf(EventingDefinitionException.class)
                .hasMessageContaining("VAL_EVENTING_UNSUPPORTED_FEATURE");
    }

    @Test
    void channel_happyPath_parses() {
        ChannelDefinition def = (ChannelDefinition) EventingDefinitionParser.parseChannel("c", CHANNEL_OK);
        assertThat(def.key()).isEqualTo("orderChannel");
        assertThat(def.subject()).isEqualTo("order.new");
        assertThat(def.eventKey()).isEqualTo("orderEvent");
        assertThat(def.queueGroup()).isEqualTo("eventing-orders");
        assertThat(def.maxDeliver()).isEqualTo(5);
    }

    @Test
    void channel_deliverGroup_forbidden() {
        assertThatThrownBy(() -> EventingDefinitionParser.parseChannel("c",
                CHANNEL_OK.replace("\"queueGroup\"", "\"deliverGroup\"")))
                .isInstanceOf(EventingDefinitionException.class)
                .hasMessageContaining("deliverGroup");
    }

    @Test
    void channel_missingQueueGroup_rejected_withMigrationHint() {
        assertThatThrownBy(() -> EventingDefinitionParser.parseChannel("c",
                CHANNEL_OK.replace("\"queueGroup\": \"eventing-orders\",", "")))
                .isInstanceOf(EventingDefinitionException.class)
                .hasMessageContaining("VAL_EVENTING_QUEUE_GROUP_REQUIRED")
                .hasMessageContaining("correlation-<messageName>");
    }

    @Test
    void channel_unsupportedKeyDetection_rejected() {
        assertThatThrownBy(() -> EventingDefinitionParser.parseChannel("c",
                CHANNEL_OK.replace("\"fixedValue\": \"orderEvent\"",
                        "\"jsonField\": \"eventKey\"")))
                .isInstanceOf(EventingDefinitionException.class)
                .hasMessageContaining("fixedValue only");
    }

    @Test
    void channel_reservedSubject_throwsNamespaceCollision() {
        assertThatThrownBy(() -> EventingDefinitionParser.parseChannel("c",
                CHANNEL_OK.replace("order.new", "jobs.orders")))
                .isInstanceOf(TopicNamespaceCollisionException.class);
        assertThatThrownBy(() -> EventingDefinitionParser.parseChannel("c",
                CHANNEL_OK.replace("order.new", "events.x.y.z")))
                .isInstanceOf(TopicNamespaceCollisionException.class);
        assertThatThrownBy(() -> EventingDefinitionParser.parseChannel("c",
                CHANNEL_OK.replace("order.new", "ewjobs.orders")))
                .isInstanceOf(TopicNamespaceCollisionException.class);
    }

    @Test
    void channel_outbound_withoutMessageType_rejected() {
        // Outbound channels are supported since slice 6 (docs/12 D-D v2) but must carry the
        // lineage extension — without extension.messageType the file would be inert.
        assertThatThrownBy(() -> EventingDefinitionParser.parseChannel("c.channel",
                CHANNEL_OK.replace("\"inbound\"", "\"outbound\"")))
                .isInstanceOf(EventingDefinitionException.class)
                .hasMessageContaining("extension.messageType");
    }

    @Test
    void channel_outbound_parsesClassificationAndAllowlist() {
        Object parsed = EventingDefinitionParser.parseChannel("out.channel", """
                { "key": "orderOut", "channelType": "outbound", "type": "nats",
                  "subject": "ignored.on.lineage",
                  "extension": { "messageType": "order-created", "critical": true,
                                 "variableAllowlist": ["orderId", "amount"] } }
                """);
        assertThat(parsed).isInstanceOf(OutboundOverlayDefinition.class);
        OutboundOverlayDefinition outbound = (OutboundOverlayDefinition) parsed;
        assertThat(outbound.key()).isEqualTo("orderOut");
        assertThat(outbound.messageType()).isEqualTo("order-created");
        assertThat(outbound.critical()).isTrue();
        assertThat(outbound.variableAllowlist()).containsExactly("orderId", "amount");
    }

    @Test
    void channel_outbound_defaultsAreBestEffortAndEmptyAllowlist() {
        OutboundOverlayDefinition outbound = (OutboundOverlayDefinition)
                EventingDefinitionParser.parseChannel("out.channel", """
                { "key": "orderOut", "channelType": "outbound", "type": "nats",
                  "extension": { "messageType": "order-created" } }
                """);
        assertThat(outbound.critical()).isFalse();
        assertThat(outbound.variableAllowlist()).isEmpty();
    }

    @Test
    void channel_unknownChannelType_rejected() {
        assertThatThrownBy(() -> EventingDefinitionParser.parseChannel("c.channel",
                CHANNEL_OK.replace("\"inbound\"", "\"sideways\"")))
                .isInstanceOf(EventingDefinitionException.class)
                .hasMessageContaining("sideways");
    }

    @Test
    void channel_ignoredFields_parseWithWarn_notReject() {
        ChannelDefinition def = (ChannelDefinition) EventingDefinitionParser.parseChannel("c",
                CHANNEL_OK.replace("\"subject\": \"order.new\"",
                        "\"subject\": \"order.new\", \"ackWaitSeconds\": 45, \"deliverPolicy\": \"new\""));
        assertThat(def.subject()).isEqualTo("order.new");
    }
}
