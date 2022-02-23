package org.ekbana.server.common.cm.request;

import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
public class ProducerRecordWriteRequest extends KafkaClientRequest {

    private String topicName;
    private String key;
    private String partitionId;

    private List<?> producerRecords;

    public ProducerRecordWriteRequest() {
        super(RequestType.PRODUCER_RECORD_WRITE);
    }
}
