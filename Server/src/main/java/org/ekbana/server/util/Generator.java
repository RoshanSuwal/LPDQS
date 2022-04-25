package org.ekbana.server.util;

public class Generator {
    private static Long transactionId=0L;

    public static Long generateTransactionId(){
        transactionId=transactionId+1;
        return transactionId;
    }
}
