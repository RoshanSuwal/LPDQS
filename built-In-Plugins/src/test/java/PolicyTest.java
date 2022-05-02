//import org.ekbana.minikafka.common.ConsumerRecords;
//import org.ekbana.minikafka.common.SegmentMetaData;
//import org.ekbana.minikafka.plugin.policy.Policy;
//import org.ekbana.minikafka.plugins.factory.*;
//import org.junit.Assert;
//import org.junit.Test;
//
//import java.time.Instant;
//import java.util.Properties;
//
//public class PolicyTest {
//
//    @Test
//    public void countBasedConsumerRecordBatchPolicyTest(){
//        Properties properties=new Properties();
//
//        final CountBasedConsumerRecordBatchPolicyFactory countBasedConsumerRecordBatchPolicyFactory = new CountBasedConsumerRecordBatchPolicyFactory();
//        final Policy<ConsumerRecords> consumerRecordsPolicy = countBasedConsumerRecordBatchPolicyFactory.buildPolicy(properties);
//
//        final ConsumerRecords consumerRecords = new ConsumerRecords();
//        consumerRecords.setSize(100);
//        consumerRecords.setRecordsCount(1);
//
//        Assert.assertTrue(consumerRecordsPolicy.validate(consumerRecords));
//        consumerRecords.setRecordsCount(10);
//        Assert.assertFalse(consumerRecordsPolicy.validate(consumerRecords));
//    }
//
//    @Test
//    public void sizeBasedConsumerRecordBatchPolicyTest(){
//        Properties properties=new Properties();
//
//        final SizeBasedConsumerRecordBatchPolicyFactory sizeBasedConsumerRecordBatchPolicyFactory = new SizeBasedConsumerRecordBatchPolicyFactory();
//        final Policy<ConsumerRecords> consumerRecordsPolicy = sizeBasedConsumerRecordBatchPolicyFactory.buildPolicy(properties);
//
//        final ConsumerRecords consumerRecords = new ConsumerRecords();
//        consumerRecords.setSize(100);
//        consumerRecords.setRecordsCount(10);
//
//        Assert.assertTrue(consumerRecordsPolicy.validate(consumerRecords));
//        consumerRecords.setSize(5232880);
//        Assert.assertTrue(consumerRecordsPolicy.validate(consumerRecords));
//
//        consumerRecords.setSize(5242880);
//        Assert.assertFalse(consumerRecordsPolicy.validate(consumerRecords));
//    }
//
//    @Test
//    public void countBasedSegmentBatchPolicyTest(){
//        Properties properties=new Properties();
//
//        final CountBasedSegmentBatchPolicyFactory countBasedSegmentBatchPolicyFactory = new CountBasedSegmentBatchPolicyFactory();
//        final Policy<SegmentMetaData> segmentMetaDataPolicy = countBasedSegmentBatchPolicyFactory.buildPolicy(properties);
//
//        final SegmentMetaData segmentMetaData = SegmentMetaData.builder()
//                .segmentId(0)
//                .segmentSize(100)
//                .offsetCount(1)
//                .build();
//
//        Assert.assertTrue(segmentMetaDataPolicy.validate(segmentMetaData));
//        segmentMetaData.setOffsetCount(9);
//        Assert.assertTrue(segmentMetaDataPolicy.validate(segmentMetaData));
//        segmentMetaData.setOffsetCount(11);
//        Assert.assertFalse(segmentMetaDataPolicy.validate(segmentMetaData));
//
//    }
//
//    @Test
//    public void sizeBasedSegmentBatchPolicyTest(){
//        Properties properties=new Properties();
//
//        final SizeBasedSegmentBatchPolicyFactory sizeBasedSegmentBatchPolicyFactory = new SizeBasedSegmentBatchPolicyFactory();
//        final Policy<SegmentMetaData> segmentMetaDataPolicy = sizeBasedSegmentBatchPolicyFactory.buildPolicy(properties);
//
//        final SegmentMetaData segmentMetaData = SegmentMetaData.builder()
//                .segmentId(0)
//                .segmentSize(100)
//                .offsetCount(1)
//                .build();
//
//        Assert.assertTrue(segmentMetaDataPolicy.validate(segmentMetaData));
//        segmentMetaData.setSegmentSize(5142880);
//        Assert.assertTrue(segmentMetaDataPolicy.validate(segmentMetaData));
//        segmentMetaData.setSegmentSize(5242880);
//        Assert.assertFalse(segmentMetaDataPolicy.validate(segmentMetaData));
//    }
//
//    @Test
//    public void timeBasedSegmentRetentionPolicyTest(){
//        Properties properties=new Properties();
//        final TimeBasedSegmentRetentionPolicyFactory timeBasedSegmentRetentionPolicyFactory = new TimeBasedSegmentRetentionPolicyFactory();
//        final Policy<SegmentMetaData> segmentRetentionPolicy = timeBasedSegmentRetentionPolicyFactory.buildPolicy(properties);
//
//        final SegmentMetaData segmentMetaData = SegmentMetaData.builder()
//                .segmentId(0)
//                .segmentSize(100)
//                .offsetCount(1)
//                .currentTimeStamp(1234)
//                .build();
//
//        Assert.assertFalse(segmentRetentionPolicy.validate(segmentMetaData));
//        segmentMetaData.setCurrentTimeStamp(Instant.now().toEpochMilli()-5000);
//        Assert.assertTrue(segmentRetentionPolicy.validate(segmentMetaData));
//    }
//}
