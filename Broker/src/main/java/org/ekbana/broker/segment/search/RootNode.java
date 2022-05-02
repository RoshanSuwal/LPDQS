package org.ekbana.broker.segment.search;

import org.ekbana.broker.utils.BrokerLogger;
import org.ekbana.minikafka.common.SegmentMetaData;
import org.ekbana.minikafka.plugin.policy.Policy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public class RootNode {

    private final List<Node> nodes;
    private final List<InternalNode> internalNodes;
    private final int n;
    private InternalNode internalNode;

    public RootNode(int n) {
        this.n = n;
        internalNodes = new ArrayList<>();
        nodes = new ArrayList<>();
        internalNode = new InternalNode();
        internalNodes.add(internalNode);
    }

    public void addLeaf(Node node,Leaf leaf) {
        if (!internalNode.hasCapacity(n)) {
            // add new internal node
            internalNode = new InternalNode();
            nodes.add(node);
            internalNodes.add(internalNode);
        }
        internalNode.addLeaf(node,leaf);
    }

    public SegmentMetaData search(long offset, boolean isTimeStamp, Policy<SegmentMetaData> policy) {
        int i = 0;
        while (nodes.size() > i) {
            if (nodes.get(i).getStatus().getPlain() && policy != null && !policy.validate(nodes.get(i).getSegmentMetaData())) {
                //node ready to get removed
                nodes.get(i).setStatus(false);
//                nodes.remove(i);
//                internalNodes.remove(i);
            } else {
                if (nodes.get(i).contain(offset, isTimeStamp)) {
                    return internalNodes.get(i).search(offset, isTimeStamp, policy);
                }
            }
            i = i + 1;
        }
        return internalNode.search(offset, isTimeStamp, policy);
    }

    public void print() {
        for (int i = 0; i < nodes.size(); i++) {
            internalNodes.get(i).print();
            System.out.println("\t\t\t" + nodes.get(i));
        }
        internalNode.print();
    }

    public void reEvaluate(Policy<SegmentMetaData> policy) {
        if (nodes.size()>0) {
            if (policy.validate(nodes.get(0).getSegmentMetaData())) {
                internalNodes.get(0).reEvaluate(policy);
            } else {
//                System.out.println("removing : "+nodes.get(0));
                BrokerLogger.searchTreeLogger.debug("Removing Root Node : {}",nodes.get(0));
                internalNodes.remove(0);
                nodes.remove(0);
                reEvaluate(policy);
            }
        }
    }

    public Stream<SegmentMetaData> transverse(){
        return internalNodes.stream()
                .flatMap(InternalNode::transverse);
    }

//    public static void main(String[] args) {
//        RootNode rootNode = new RootNode(2);
//        rootNode.addLeaf(
//                new Node(1,100,new AtomicBoolean(true)),
//                new Leaf(SegmentMetaData.builder().startingOffset(1).currentOffset(10).startingTimeStamp(100).currentTimeStamp(200).build()));
//        sleep(1);
//        rootNode.addLeaf(new Node(10, Instant.now().getEpochSecond(),new AtomicBoolean(true)),new Leaf(SegmentMetaData.builder().startingOffset(11).currentOffset(20).startingTimeStamp(201).currentTimeStamp(300).build()));
//        sleep(2);
//        rootNode.addLeaf(new Node(20,Instant.now().getEpochSecond(),new AtomicBoolean(true)),new Leaf(SegmentMetaData.builder().startingOffset(21).currentOffset(30).startingTimeStamp(301).currentTimeStamp(400).build()));
//        sleep(1);
//        rootNode.addLeaf(new Node(30,Instant.now().getEpochSecond(),new AtomicBoolean(true)),new Leaf(SegmentMetaData.builder().startingOffset(31).currentOffset(40).startingTimeStamp(401).currentTimeStamp(500).build()));
//        sleep(1);
//        rootNode.addLeaf(new Node(40,Instant.now().getEpochSecond(),new AtomicBoolean(true)),new Leaf(SegmentMetaData.builder().startingOffset(41).currentOffset(50).startingTimeStamp(501).currentTimeStamp(600).build()));
//        sleep(1);
//        rootNode.addLeaf(new Node(50,Instant.now().getEpochSecond(),new AtomicBoolean(true)),new Leaf(SegmentMetaData.builder().startingOffset(51).currentOffset(60).startingTimeStamp(601).currentTimeStamp(700).build()));
//        sleep(1);
//        rootNode.addLeaf(new Node(60,Instant.now().getEpochSecond(),new AtomicBoolean(true)),new Leaf(SegmentMetaData.builder().startingOffset(61).currentOffset(70).startingTimeStamp(701).currentTimeStamp(800).build()));
//        sleep(1);
//        rootNode.addLeaf(new Node(70,Instant.now().getEpochSecond(),new AtomicBoolean(true)),new Leaf(SegmentMetaData.builder().startingOffset(71).currentOffset(80).startingTimeStamp(801).currentTimeStamp(900).build()));
//        sleep(1);
//        rootNode.addLeaf(new Node(80,Instant.now().getEpochSecond(),new AtomicBoolean(true)),new Leaf(SegmentMetaData.builder().startingOffset(81).currentOffset(90).startingTimeStamp(901).currentTimeStamp(1000).build()));
//        rootNode.print();
//        final SegmentRetentionPolicy policy = new SegmentRetentionPolicy();
//        System.out.println("\n\n re evaluation \n");
//        rootNode.reEvaluate(policy);
//        rootNode.print();
////        System.out.println();
////        System.out.println(rootNode.search(5, false, policy));
////        System.out.println(rootNode.search(29, false, policy));
////        System.out.println(rootNode.search(30, false, policy));
////        System.out.println(rootNode.search(200, true, policy));
////        System.out.println(rootNode.search(1000, true, policy));
//
//    }

    public static void sleep(long time){
        try {
            Thread.sleep(time*1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
