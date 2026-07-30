package com.threeai.nats.camunda.config;

import java.util.ArrayList;
import java.util.List;

import com.threeai.nats.camunda.inbound.SubscriptionConfig;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "spring.nats.camunda")
public class CamundaNatsProperties {

    private List<SubscriptionConfig> subscriptions = new ArrayList<>();

    public List<SubscriptionConfig> getSubscriptions() {
        return subscriptions;
    }

    public void setSubscriptions(List<SubscriptionConfig> subscriptions) {
        this.subscriptions = subscriptions;
    }
}
