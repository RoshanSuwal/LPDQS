package org.ekbana.server.common.cm.request;

public class ConsumerCommitOffsetRequest extends KafkaClientRequest {

    public ConsumerCommitOffsetRequest() {
        super(RequestType.CONSUMER_OFFSET_COMMIT);
    }
}
