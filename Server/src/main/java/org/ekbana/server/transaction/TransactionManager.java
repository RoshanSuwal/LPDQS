package org.ekbana.server.transaction;

import java.time.Instant;
import java.util.HashMap;
import java.util.OptionalLong;

public class TransactionManager extends Scheduler.SchedulerCallBack {
    // manages the transaction
    // wait for t-time period for transaction to complete
    //
    private final Long TTL=1000L;
    private final HashMap<Long,Transaction> transactionHashMap=new HashMap<>();
    private final HashMap<Long,Long> transactionTTL=new HashMap<>();

    public void registerTransaction(Transaction transaction){
        transactionHashMap.put(transaction.getId(),transaction);
        transactionTTL.put(transaction.getId(), Instant.now().toEpochMilli()+TTL);
    }

    public void transactionProcess(Transaction transaction){

    }

    private void TTL(){
        new Thread(()->{
            while (transactionHashMap.size()>0){
                final OptionalLong min = transactionTTL.values()
                        .stream()
                        .mapToLong(ttl -> ttl)
                        .min();
            }
        }).start();
    }


    @Override
    protected void schedule() {
        if (transactionHashMap.size()>0){
            transactionTTL.values()
                    .stream()
                    .filter(ttl->Instant.now().getEpochSecond()>ttl)
                    .peek(ttl->transactionHashMap.remove(transactionTTL.get(ttl)))
                    .forEach(transactionTTL::remove);
        }
    }
}
