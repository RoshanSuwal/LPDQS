package org.ekbana.server.common.cm.request;

import lombok.Getter;

@Getter
public class TopicDeleteRequest extends KafkaClientRequest{
    private final String topicName;
    public TopicDeleteRequest(String topicName) {
        super(RequestType.TOPIC_DELETE);
        this.topicName = topicName;
    }
}
