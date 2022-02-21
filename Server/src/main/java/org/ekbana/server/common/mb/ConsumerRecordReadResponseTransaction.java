package org.ekbana.server.common.mb;

public class ConsumerRecordReadResponseTransaction extends BaseResponseTransaction{

    private ConsumerRecords consumerRecords;

    public ConsumerRecordReadResponseTransaction(long transactionId, TransactionType.Action action) {
        super(transactionId, action);
    }
}
