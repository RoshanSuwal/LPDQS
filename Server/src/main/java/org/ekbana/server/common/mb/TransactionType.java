package org.ekbana.server.common.mb;

public class TransactionType {
    public enum Action {
        REGISTER, COMMIT, ABORT,
        ACKNOWLEDGE, PASS, SUCCESS, FAIL
    }

    public enum RequestType{
        TOPIC_PARTITION_CREATE,
        TOPIC_PARTITION_DELETE,
        PRODUCER_RECORD_WRITE,
        CONSUMER_RECORD_READ
    }
}
