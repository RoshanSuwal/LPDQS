package org.ekbana.server.common.cm.request;

public class NewConnectionRequest extends KafkaClientRequest {
    public NewConnectionRequest() {
        super(RequestType.NEW_CONNECTION);
    }
}
