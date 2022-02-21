package org.ekbana.server.common.cm.request;

public class ConsumerRecordReadRequest extends KafkaClientRequest{
    public ConsumerRecordReadRequest() {
        super(RequestType.CONSUMER_RECORD_READ);
    }
}
