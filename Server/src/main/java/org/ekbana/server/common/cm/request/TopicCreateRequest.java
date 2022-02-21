package org.ekbana.server.common.cm.request;

import lombok.Getter;

@Getter
public class TopicCreateRequest extends KafkaClientRequest {
    private final String topicName;
    private final int numberOfPartitions;

    public TopicCreateRequest(String topicName, int numberOfPartitions) {
        super(RequestType.TOPIC_CREATE);
        this.topicName = topicName;
        this.numberOfPartitions = numberOfPartitions;
    }
}
