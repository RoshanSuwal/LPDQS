package org.ekbana.server.common.cm.response;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.ekbana.server.common.cm.request.KafkaClientRequest;
import org.ekbana.server.common.mb.ConsumerRecords;

@Getter @Setter @ToString
public class ConsumerRecordResponse extends KafkaClientResponse{
    private ConsumerRecords consumerRecords;

    public ConsumerRecordResponse(Long clientResponseId, KafkaClientRequest.RequestType requestType, ResponseType responseType,ConsumerRecords consumerRecords) {
        super(clientResponseId, requestType, responseType);
        this.consumerRecords=consumerRecords;
    }
}
