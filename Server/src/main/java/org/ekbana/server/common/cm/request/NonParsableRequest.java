package org.ekbana.server.common.cm.request;

public class NonParsableRequest extends KafkaClientRequest {

    public NonParsableRequest() {
        super(RequestType.NON_PARSABLE);
    }
}
