package org.ekbana.server.common.mb;

public class BaseRequestTransaction extends Transaction {
    public BaseRequestTransaction(long transactionId, TransactionType.Action action) {
        super(transactionId, action);
    }
}
