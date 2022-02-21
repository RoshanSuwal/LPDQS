package org.ekbana.server.common.mb;

public class AbortRequestTransaction extends Transaction{

    public AbortRequestTransaction(long transactionId) {
        super(transactionId, TransactionType.Action.ABORT);
    }
}
