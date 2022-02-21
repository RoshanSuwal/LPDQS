package org.ekbana.server.common.mb;

public class BaseResponseTransaction extends Transaction {
    public BaseResponseTransaction(long transactionId, TransactionType.Action action) {
        super(transactionId, action);
    }
}
