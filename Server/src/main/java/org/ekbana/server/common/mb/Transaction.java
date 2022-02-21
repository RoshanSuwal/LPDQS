package org.ekbana.server.common.mb;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

import java.io.Serializable;

@AllArgsConstructor
@Getter
@ToString
public abstract class Transaction implements Serializable {
    private final long transactionId;
    private final TransactionType.Action action;
}
