package com.threeai.nats.history.projection;

import java.util.ArrayList;
import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * {@code history.projection.*} (`08_config.md` §4). {@code partitionAssignment}, when non-empty,
 * lists the partition indices THIS replica owns directly (embeddable / single-node deployments
 * typically list all {@code [0..partitionCount)}); when empty, the owning partition is derived
 * from the K8s StatefulSet replica ordinal (`01_overview.md` "#2": {@code i = replicaOrdinal % N}).
 */
@ConfigurationProperties(prefix = "history.projection")
public class HistoryProjectionProperties {

    private int partitionCount = 8;
    private List<Integer> partitionAssignment = new ArrayList<>();

    public int getPartitionCount() {
        return partitionCount;
    }

    public void setPartitionCount(int partitionCount) {
        this.partitionCount = partitionCount;
    }

    public List<Integer> getPartitionAssignment() {
        return partitionAssignment;
    }

    public void setPartitionAssignment(List<Integer> partitionAssignment) {
        this.partitionAssignment = partitionAssignment;
    }
}
