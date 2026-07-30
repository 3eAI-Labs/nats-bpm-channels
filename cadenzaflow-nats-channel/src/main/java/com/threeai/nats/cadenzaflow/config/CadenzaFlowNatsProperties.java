package com.threeai.nats.cadenzaflow.config;

import java.util.ArrayList;
import java.util.List;

import com.threeai.nats.cadenzaflow.inbound.SubscriptionConfig;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "spring.nats.cadenzaflow")
public class CadenzaFlowNatsProperties {

    private List<SubscriptionConfig> subscriptions = new ArrayList<>();

    public List<SubscriptionConfig> getSubscriptions() {
        return subscriptions;
    }

    public void setSubscriptions(List<SubscriptionConfig> subscriptions) {
        this.subscriptions = subscriptions;
    }
}
