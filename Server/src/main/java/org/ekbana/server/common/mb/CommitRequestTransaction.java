package org.ekbana.server.common.mb;

public class CommitRequestTransaction extends Transaction {

    public CommitRequestTransaction(long transactionId) {
        super(transactionId, TransactionType.Action.COMMIT);
    }
}
