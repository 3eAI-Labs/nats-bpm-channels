package org.flowable.eventregistry.spring.nats;

import static org.assertj.core.api.Assertions.assertThat;

import org.flowable.eventregistry.impl.EventRegistryEngineConfiguration;
import org.flowable.eventregistry.json.converter.ChannelJsonConverter;
import org.flowable.eventregistry.json.converter.EventJsonConverter;
import org.flowable.eventregistry.model.EventModel;
import org.flowable.eventregistry.spring.nats.channel.NatsInboundChannelModel;
import org.junit.jupiter.api.Test;

/**
 * Gate G4 (docs/12): portability = PARSED-MODEL EQUALITY. The two JSON strings below are
 * byte-identical to the pair the Camunda-lineage acceptance suites deploy
 * ({@code EventingParityAcceptanceTest} in the camunda/cibseven/cadenzaflow modules). Here the
 * SAME strings go through Flowable's own strict databind, and the parsed models must carry the
 * same key facts: subject, queueGroup (the shared field name), event-key detection, correlation
 * parameters and payload types. The {@code extension} block rides Flowable's escape hatch; the
 * lineage reads {@code extension.messageName}, Flowable ignores it.
 */
class PortableDefinitionParityTest {

    // byte-identical to EventingParityAcceptanceTest.EVENT_JSON (lineage modules)
    private static final String EVENT_JSON = """
            { "key": "orderEvent", "name": "Order Event",
              "correlationParameters": [ { "name": "orderId", "type": "string" } ],
              "payload": [ { "name": "amount", "type": "double" } ],
              "extension": { "messageName": "OrderMessage" } }
            """;

    // byte-identical to EventingParityAcceptanceTest.CHANNEL_JSON (lineage modules)
    private static final String CHANNEL_JSON = """
            { "key": "orderChannel", "channelType": "inbound", "type": "nats",
              "channelEventKeyDetection": { "fixedValue": "orderEvent" },
              "queueGroup": "eventing-orders", "subject": "evt.order.accept" }
            """;

    @Test
    void portableChannel_parsesInFlowablesStrictDatabind_withEqualModelFacts() {
        EventRegistryEngineConfiguration engineConfiguration = new EventRegistryEngineConfiguration();
        ChannelJsonConverter converter = engineConfiguration.getChannelJsonConverter();
        converter.addInboundChannelModelClass("nats", NatsInboundChannelModel.class);

        NatsInboundChannelModel model =
                (NatsInboundChannelModel) converter.convertToChannelModel(CHANNEL_JSON);

        assertThat(model.getKey()).isEqualTo("orderChannel");
        assertThat(model.getChannelType()).isEqualTo("inbound");
        assertThat(model.getType()).isEqualTo("nats");
        assertThat(model.getSubject()).isEqualTo("evt.order.accept");
        assertThat(model.getQueueGroup()).isEqualTo("eventing-orders");
        assertThat(model.getChannelEventKeyDetection().getFixedValue()).isEqualTo("orderEvent");
    }

    @Test
    void portableEvent_parsesInFlowablesConverter_withEqualModelFacts() {
        EventModel model = new EventJsonConverter().convertToEventModel(EVENT_JSON);

        assertThat(model.getKey()).isEqualTo("orderEvent");
        assertThat(model.getName()).isEqualTo("Order Event");
        assertThat(model.getCorrelationParameters()).singleElement().satisfies(p -> {
            assertThat(p.getName()).isEqualTo("orderId");
            assertThat(p.getType()).isEqualTo("string");
        });
        assertThat(model.getPayload()).extracting("name", "type")
                .contains(org.assertj.core.groups.Tuple.tuple("amount", "double"));
    }
}
