package org.ekbana.server.leader;

import org.ekbana.server.common.mb.RequestTransaction;

import java.util.Arrays;

public class TransactionHelper {
    private final Object obj;
    private final Node[] nodes;
    private final ThreePhaseTransactionStatus[] threePhaseTransactionStatuses;

    public TransactionHelper(Object obj,Node[] nodes){
        this.obj=obj;
        this.nodes=nodes;
//        this.nodes=requestTransaction.getPartitionNodes();
        threePhaseTransactionStatuses=new ThreePhaseTransactionStatus[nodes.length];
        Arrays.fill(threePhaseTransactionStatuses, ThreePhaseTransactionStatus.SEND);
    }

    public Object getObj(){
        return obj;
    }

    public ThreePhaseTransactionStatus getThreePhaseTransactionStatus(Node node){
        for (int i=0;i<nodes.length;i++){
            if (node.equals(nodes[i]))
                return threePhaseTransactionStatuses[i];
        }
        return null;
    }

    public void setThreePhaseTransactionStatuses(Node node,ThreePhaseTransactionStatus threePhaseTransactionStatus){
        for (int i=0;i<nodes.length;i++){
            if (node.equals(nodes[i]))
                threePhaseTransactionStatuses[i]=threePhaseTransactionStatus;
        }
    }

    public boolean isReadyForCommit(){
        return Arrays.stream(threePhaseTransactionStatuses).
                allMatch(threePhaseTransactionStatus -> threePhaseTransactionStatus==ThreePhaseTransactionStatus.ACK);
    }
}
