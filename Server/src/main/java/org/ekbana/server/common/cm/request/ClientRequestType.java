package org.ekbana.server.common.cm.request;

public enum ClientRequestType {
    AUTH,
    TOPIC_CREATE,TOPIC_DELETE,
    PRODUCER_CONFIG,PRODUCER_RECORD_WRITE,
    CONSUMER_CONFIG,CONSUMER_RECORD_READ,
    CONSUMER_OFFSET_COMMIT,
    CLOSE,INVALID
}
