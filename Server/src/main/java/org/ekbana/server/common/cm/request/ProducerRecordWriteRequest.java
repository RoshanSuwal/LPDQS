package org.ekbana.server.common.cm.request;

public class ProducerRecordWriteRequest extends KafkaClientRequest {

    public ProducerRecordWriteRequest() {
        super(RequestType.PRODUCER_RECORD_WRITE);
    }
}
