package org.ekbana.server.common.cm.response;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.ekbana.server.common.cm.request.KafkaClientRequest;
import org.ekbana.server.common.mb.Topic;

@Getter @Setter @ToString
public class ConsumerConfigResponse extends KafkaClientResponse {

    private Topic topic;
    public ConsumerConfigResponse(Long clientResponseId, Topic topic) {
        super(clientResponseId, KafkaClientRequest.RequestType.CONSUMER_CONFIG, ResponseType.SUCCESS);
        this.topic=topic;

    }

}
