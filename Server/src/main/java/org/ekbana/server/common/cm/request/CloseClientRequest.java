package org.ekbana.server.common.cm.request;

public class CloseClientRequest extends KafkaClientRequest{
    public CloseClientRequest() {
        super(RequestType.CLOSE_CLIENT);
    }
}
