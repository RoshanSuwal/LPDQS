//package org.ekbana.broker.Policy;
//
//import org.ekbana.broker.segment.search.Node;
//
//public class SegmentRetentionPolicy implements Policy<Node> {
//    private final long criteria=60*1000;
//    private long compareWith=0;
//
//    public void setCompareWith(long compareWith){
//        this.compareWith=compareWith;
////        System.out.println("policy "+ compareWith);
//    }
//
//    @Override
//    public boolean validate(Node node) {
////        System.out.println("comparing node "+ node);
//        return compareWith-node.getCriteria()<=criteria;
//    }
//    // timeStampBased Policy -- stores upto n-interval -- need to be defined
//    // size based policy -- stores maximum of n-bytes -- reevaluation done only during creation of segment
//    // count based policy -- stores maximum of n segments -- reevaluation done only during the creation of segment
//
//
//}
