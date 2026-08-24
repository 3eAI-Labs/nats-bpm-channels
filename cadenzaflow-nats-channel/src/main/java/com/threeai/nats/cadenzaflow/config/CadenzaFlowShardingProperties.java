package com.threeai.nats.cadenzaflow.config;

import java.util.ArrayList;
import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * {@code spring.nats.cadenzaflow.sharding.*} — the instance-sharding gate (docs/13 D-F house
 * rule: default OFF; with {@code enabled=false} nothing shard-shaped exists — no topology,
 * no guard, no router, no validator; behavior is bit-for-bit the unsharded adapter).
 */
@ConfigurationProperties(prefix = "spring.nats.cadenzaflow.sharding")
public class CadenzaFlowShardingProperties {

    /** Master gate. The capability activates only when set explicitly. */
    private boolean enabled = false;

    /** N — fixed for the fleet's lifetime (D-D v4; growth = parallel-fleet, docs/14). */
    private int shardCount = 1;

    /** This deployment's shard id, in [0, shardCount). */
    private int shardId = 0;

    /** External inbound subjects the router owns (each with an optional payload key field). */
    private List<Route> routes = new ArrayList<>();

    /** Router consumer budget — one side of the duplicate-window invariant (T-8). */
    private long routerAckWaitSeconds = 30;
    private int routerMaxDeliver = 5;

    public static class Route {
        private String subject;
        private String businessKeyField;

        public String getSubject() {
            return subject;
        }

        public void setSubject(String subject) {
            this.subject = subject;
        }

        public String getBusinessKeyField() {
            return businessKeyField;
        }

        public void setBusinessKeyField(String businessKeyField) {
            this.businessKeyField = businessKeyField;
        }
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public int getShardCount() {
        return shardCount;
    }

    public void setShardCount(int shardCount) {
        this.shardCount = shardCount;
    }

    public int getShardId() {
        return shardId;
    }

    public void setShardId(int shardId) {
        this.shardId = shardId;
    }

    public List<Route> getRoutes() {
        return routes;
    }

    public void setRoutes(List<Route> routes) {
        this.routes = routes;
    }

    public long getRouterAckWaitSeconds() {
        return routerAckWaitSeconds;
    }

    public void setRouterAckWaitSeconds(long routerAckWaitSeconds) {
        this.routerAckWaitSeconds = routerAckWaitSeconds;
    }

    public int getRouterMaxDeliver() {
        return routerMaxDeliver;
    }

    public void setRouterMaxDeliver(int routerMaxDeliver) {
        this.routerMaxDeliver = routerMaxDeliver;
    }
}
