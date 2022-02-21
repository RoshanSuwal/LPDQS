package org.ekbana.server.leader;

import lombok.RequiredArgsConstructor;
import org.ekbana.server.common.mb.RequestTransaction;
import org.ekbana.server.util.Mapper;

@RequiredArgsConstructor
public class TransactionManager {
    private final Mapper<Long,TransactionHelper> transactionHelperMapper;

    public Node[] getPartitionNodes(long transactionId){
        return transactionHelperMapper.get(transactionId).getRequestTransaction().getPartitionNodes();
    }

    public boolean hasTransaction(long transactionId){
        return transactionHelperMapper.has(transactionId);
    }
    public void registerTransaction(RequestTransaction requestTransaction){
        transactionHelperMapper.add(requestTransaction.getTransactionId(),new TransactionHelper(requestTransaction));
    }

    public void deleteTransaction(long transactionId){
        transactionHelperMapper.delete(transactionId);
    }

    public void updateTransaction3PhaseStatus(long transactionId,Node node,ThreePhaseTransactionStatus status){
        transactionHelperMapper.get(transactionId).setThreePhaseTransactionStatuses(node,status);
    }

    public boolean readyToCommit(long transactionId){
        return transactionHelperMapper.get(transactionId).isReadyForCommit();
    }
}