package org.ekbana.server.common.mb;

public class AcknowledgeResponseTransaction extends Transaction {
    public AcknowledgeResponseTransaction(long transactionId) {
        super(transactionId, TransactionType.Action.ACKNOWLEDGE);
    }
}
