package org.ekbana.server.common.cm.request;

import lombok.ToString;

import java.io.Serializable;

@ToString
public abstract class KafkaClientRequest implements Serializable {

    public enum RequestType {
        AUTH,
        TOPIC_CREATE, TOPIC_DELETE,
        PRODUCER_CONFIG, PRODUCER_RECORD_WRITE,
        CONSUMER_CONFIG, CONSUMER_RECORD_READ,
        CONSUMER_OFFSET_COMMIT,
        CLOSE_CLIENT, INVALID,
        NON_PARSABLE,NEW_CONNECTION
    }

    private final KafkaClientRequest.RequestType requestType;
    private Long clientRequestId;

    public KafkaClientRequest(KafkaClientRequest.RequestType requestType) {
        this.requestType = requestType;
    }

    public KafkaClientRequest.RequestType getRequestType() {
        return requestType;
    }

    public Long getClientRequestId() {
        return clientRequestId;
    }

    public void setClientRequestId(Long clientRequestId) {
        this.clientRequestId = clientRequestId;
    }
}
