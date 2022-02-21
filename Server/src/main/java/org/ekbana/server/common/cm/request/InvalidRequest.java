package org.ekbana.server.common.cm.request;

public class InvalidRequest extends KafkaClientRequest {
    private final String errorMsg;
    public InvalidRequest(String errorMsg) {
        super(KafkaClientRequest.RequestType.INVALID);
        this.errorMsg=errorMsg;
    }

    public InvalidRequest(Long requestId,String errorMsg){
        super(RequestType.INVALID);
        this.errorMsg=errorMsg;
        this.setClientRequestId(requestId);
    }
}
