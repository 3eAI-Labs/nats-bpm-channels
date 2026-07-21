package com.threeai.nats.history.cutover;

import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** {@code history.cutover.*} (`08_config.md` §5, BR-HDL-005 volume-priority queue order). */
@ConfigurationProperties(prefix = "history.cutover")
public class HistoryCutoverProperties {

    private List<String> volumePriorityOrder = List.of("DETAIL", "VARINST", "ACTINST", "JOB_LOG", "TASKINST",
            "PROCINST", "COMMENT", "ATTACHMENT", "IDENTITYLINK", "DECINST", "CASEINST", "BATCH",
            "EXT_TASK_LOG", "INCIDENT", "OP_LOG");

    public List<String> getVolumePriorityOrder() {
        return volumePriorityOrder;
    }

    public void setVolumePriorityOrder(List<String> volumePriorityOrder) {
        this.volumePriorityOrder = volumePriorityOrder;
    }
}
