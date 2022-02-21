package org.ekbana.server.common.cm.request;

public class BaseRequest extends KafkaClientRequest{

    public BaseRequest(RequestType requestType) {
        super(requestType);
    }
}
