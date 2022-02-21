package org.ekbana.server.common.cm.request;

import lombok.Getter;

@Getter
public class ConsumerConfigRequest extends KafkaClientRequest {
    private final String topicName;
    private final String groupName;
    private final int[] partitions;

    public ConsumerConfigRequest(String topicName, String groupName, int[] partitions) {
        super(RequestType.CONSUMER_CONFIG);
        this.topicName = topicName;
        this.groupName = groupName;
        this.partitions = partitions;
    }
}
