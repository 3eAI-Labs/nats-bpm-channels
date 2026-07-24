package com.threeai.nats.cibseven.config;

import java.util.ArrayList;
import java.util.List;

import com.threeai.nats.cibseven.inbound.SubscriptionConfig;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "spring.nats.cibseven")
public class CibSevenNatsProperties {

    private List<SubscriptionConfig> subscriptions = new ArrayList<>();

    public List<SubscriptionConfig> getSubscriptions() {
        return subscriptions;
    }

    public void setSubscriptions(List<SubscriptionConfig> subscriptions) {
        this.subscriptions = subscriptions;
    }
}
