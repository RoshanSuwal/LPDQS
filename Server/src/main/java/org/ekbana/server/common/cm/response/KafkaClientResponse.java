package org.ekbana.server.common.cm.response;

import lombok.ToString;
import org.ekbana.server.common.cm.request.KafkaClientRequest;

import java.io.Serializable;

@ToString
public abstract class KafkaClientResponse implements Serializable {
    public enum ResponseType{
        SUCCESS,FAIL
    }

    private final Long clientResponseId;
    private long requestId;
    private final KafkaClientRequest.RequestType requestType;
    private final KafkaClientResponse.ResponseType responseType;

    public KafkaClientResponse(Long clientResponseId, KafkaClientRequest.RequestType requestType, ResponseType responseType) {
        this.clientResponseId = clientResponseId;
        this.requestType = requestType;
        this.responseType = responseType;
    }

    public KafkaClientRequest.RequestType getRequestType() {
        return requestType;
    }

    public KafkaClientResponse.ResponseType getResponseType() {
        return responseType;
    }

    public Long getClientResponseId() {
        return clientResponseId;
    }

    public Long getRequestId(){return requestId;}
    public void setRequestId(long requestId){ this.requestId=requestId;}
}
