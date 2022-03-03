//package org.ekbana.broker.Policy;
//
//import org.ekbana.broker.record.Records;
//
//public class ConsumerRecordBatchPolicy implements Policy<Records>{
//
//    @Override
//    public boolean validate(Records records) {
//        System.out.println("records size : "+records.size());
////        return records.count() < 5;
//        return records.size() < 1000;
//    }
//}
