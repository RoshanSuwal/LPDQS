package org.ekbana.server.transaction;

import lombok.Getter;
import lombok.Setter;

@Getter @Setter
public class Transaction {
    private long id;
    private TransactionType transactionType;
    private String topicName;
    private int partitionId;
}
