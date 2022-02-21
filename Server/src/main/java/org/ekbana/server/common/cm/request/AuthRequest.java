package org.ekbana.server.common.cm.request;

public class AuthRequest extends KafkaClientRequest {

    public AuthRequest() {
        super(RequestType.AUTH);
    }
}
