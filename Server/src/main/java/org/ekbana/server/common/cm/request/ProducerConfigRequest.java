package org.ekbana.server.common.cm.request;

import lombok.Getter;

@Getter
public class  ProducerConfigRequest extends KafkaClientRequest{
    private final String topicName;

    public ProducerConfigRequest(String topicName) {
        super(RequestType.PRODUCER_CONFIG);
        this.topicName = topicName;
    }
}
