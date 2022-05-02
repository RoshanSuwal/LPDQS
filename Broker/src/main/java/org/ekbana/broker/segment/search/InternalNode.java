package org.ekbana.broker.segment.search;

import org.ekbana.broker.Broker;
import org.ekbana.broker.utils.BrokerLogger;
import org.ekbana.minikafka.common.SegmentMetaData;
import org.ekbana.minikafka.plugin.policy.Policy;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public class InternalNode {
    private final List<Node> nodes = new ArrayList<>();
    private final List<Leaf> leaves = new ArrayList<>(); // n+1 leaves

    public long size() {
        return leaves.stream()
                .mapToLong(leaf -> leaf.getSegmentMetaData().getSegmentSize())
                .sum();
    }

    public void addLeaf(Node node, Leaf leaf) {
        if (nodes.size() < leaves.size()) {
            nodes.add(node);
        }
        leaves.add(leaf);
    }

    public boolean hasCapacity(int n) {
        return nodes.size() <= n && leaves.size() <= n + 1;
    }

    public int nodeCount() {
        return nodes.size();
    }

    public int leafCount() {
        return leaves.size();
    }

    public SegmentMetaData search(long offset, boolean isTimeStamp, Policy<SegmentMetaData> policy) {
        int i=0;
        while (nodes.size()>i){
            if (nodes.get(i).getStatus().getPlain() && policy!=null && !policy.validate(nodes.get(i).getSegmentMetaData())){
                nodes.get(i).setStatus(false);
//                nodes.remove(i);
//                leaves.remove(i);
            }else if (nodes.get(i).getStatus().getPlain()){
                if (nodes.get(i).contain(offset,isTimeStamp)){
                    return leaves.get(i).search(offset,isTimeStamp);
                }
            }
            i=i+1;
        }
        return leaves.size()==0
                ?null
                :leaves.get(leaves.size() - 1).search(offset, isTimeStamp);
    }

    public void print() {
        for (int i = 0; i < nodes.size(); i++) {
            leaves.get(i).print();
            System.out.println("\t\t" + nodes.get(i));
        }
        leaves.get(leaves.size() - 1).print();
    }

    public void reEvaluate(Policy<SegmentMetaData> policy) {
        if (nodes.size()>0){
            if (!policy.validate(nodes.get(0).getSegmentMetaData())){
                BrokerLogger.searchTreeLogger.debug("Removing internal node : {}",nodes.get(0));
                nodes.remove(0);
                leaves.remove(0);
                reEvaluate(policy);
            }
        }
    }

    public Stream<SegmentMetaData> transverse(){
        return leaves.stream().map(Leaf::transverse);
    }
}
