package org.ekbana.server.common.cm.response;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.ekbana.server.common.cm.request.KafkaClientRequest;
import org.ekbana.server.common.mb.Topic;

@Getter
@Setter
@ToString
public class ProducerConfigResponse extends KafkaClientResponse {
    private final Topic topic;

    public ProducerConfigResponse(Long clientResponseId, Topic topic) {
        super(clientResponseId, KafkaClientRequest.RequestType.PRODUCER_CONFIG, ResponseType.SUCCESS);
        this.topic=topic;
    }
}
