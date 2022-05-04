package org.ekbana.server.v2.node;

import lombok.RequiredArgsConstructor;
import org.ekbana.minikafka.common.Node;
import org.ekbana.server.common.mb.RequestTransaction;
import org.ekbana.server.common.mb.Transaction;
import org.ekbana.server.util.KafkaLogger;
import org.ekbana.server.util.Mapper;

import java.util.Arrays;
import java.util.List;

@RequiredArgsConstructor
public class TransactionManager {
    private final Mapper<Long, TransactionHelper> transactionHelperMapper;

    public Node[] getPartitionNodes(long transactionId){
        return ((RequestTransaction)transactionHelperMapper.get(transactionId).getObj()).getPartitionNodes();
    }

    public boolean hasTransaction(long transactionId){
        return transactionHelperMapper.has(transactionId);
    }

    public Transaction getTransaction(long transactionId){
        return (Transaction) transactionHelperMapper.get(transactionId).getObj();
    }

    public void registerTransaction(RequestTransaction requestTransaction){
        transactionHelperMapper.add(requestTransaction.getTransactionId(),new TransactionHelper(requestTransaction,requestTransaction.getPartitionNodes()));
    }

    public void deleteTransaction(long transactionId){
        transactionHelperMapper.delete(transactionId);
    }

    public void updateTransaction3PhaseStatus(long transactionId, Node node, ThreePhaseTransactionStatus status){
        KafkaLogger.transactionLogger.debug("[3 phase commit Update] - transactionId :{} Node : {}", transactionId,node.getId());
        transactionHelperMapper.get(transactionId).setThreePhaseTransactionStatuses(node,status);

//        System.out.println(transactionHelperMapper.get(transactionId));
    }

    public boolean readyToCommit(long transactionId){
        return transactionHelperMapper.get(transactionId).isReadyForCommit();
    }

    /** get the transactions by nodeId*/
    public List<RequestTransaction> getTransactionRunningInNode(String nodeId){
        return transactionHelperMapper.getValues()
                .stream()
                .map(transactionHelper -> (RequestTransaction)transactionHelper.getObj())
                .filter(requestTransaction-> Arrays.stream(requestTransaction.getPartitionNodes()).filter(node -> node.getId()==nodeId).toArray().length>0)
                .toList();
    }
}
