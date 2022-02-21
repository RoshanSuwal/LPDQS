package org.ekbana.server.common.cm.response;

import org.ekbana.server.common.cm.request.KafkaClientRequest;

public class BaseResponse extends KafkaClientResponse {

    private final String message;

    public BaseResponse(Long clientResponseId, KafkaClientRequest.RequestType requestType, ResponseType responseType, String message) {
        super(clientResponseId, requestType, responseType);
        this.message = message;
    }

    public String getMessage() {
        return message;
    }
}
