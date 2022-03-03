package org.ekbana.server.broker;

import org.ekbana.broker.Broker;
import org.ekbana.minikafka.common.ProducerRecords;
import org.ekbana.minikafka.common.Record;
import org.ekbana.server.cluster.Node;
import org.ekbana.server.common.Router;
import org.ekbana.server.common.mb.*;
import org.ekbana.server.config.KafkaProperties;
import org.ekbana.server.util.Helper;
import org.ekbana.server.util.Mapper;
import org.ekbana.server.util.QueueProcessor;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

public class KafkaBrokerController {
    private final KafkaProperties kafkaProperties;
    private final Broker broker;
    private final ExecutorService executorService;
    private final Router.KafkaBrokerRouter kafkaBrokerRouter;

    private final Mapper<Long,RequestTransaction> brokerTransactionMapper;

    private final QueueProcessor<Transaction> transactionQueueProcessor;

    public KafkaBrokerController(KafkaProperties kafkaProperties,Broker broker, ExecutorService executorService, Router.KafkaBrokerRouter kafkaBrokerRouter, Mapper<Long, RequestTransaction> brokerTransactionMapper) {
        this.kafkaProperties=kafkaProperties;
        this.broker = broker;
        this.executorService = executorService;
        this.kafkaBrokerRouter = kafkaBrokerRouter;
        this.brokerTransactionMapper = brokerTransactionMapper;
        QueueProcessor.QueueProcessorListener<Transaction> transactionQueueProcessorListener = this::preProcess;
        transactionQueueProcessor=new QueueProcessor<>(100, transactionQueueProcessorListener,executorService);
    }

    private void log(String fromTo,Object obj){
        System.out.println("[broker] ["+fromTo+"] "+obj);
    }

    public void request(Transaction transaction){
        log("request",transaction);
        transactionQueueProcessor.push(transaction,false);
    }

    public void preProcess(Transaction transaction){
        log("process",transaction);
        if (transaction.getAction()== TransactionType.Action.REGISTER) {
            brokerTransactionMapper.add(transaction.getTransactionId(), (RequestTransaction) transaction);
            //processing
            response(new BaseResponseTransaction(transaction.getTransactionId(), TransactionType.Action.ACKNOWLEDGE));
        }else if (transaction.getAction()==TransactionType.Action.COMMIT){
            // execute the transaction according to action
            processTransaction(brokerTransactionMapper.get(transaction.getTransactionId()));
        }else if (transaction.getAction()==TransactionType.Action.ABORT){
            brokerTransactionMapper.delete(transaction.getTransactionId());
        }
    }

    public void response(Transaction transaction){
        log("response",transaction);
        kafkaBrokerRouter.routeFromBrokerToFollower(transaction);
    }

    private void processTransaction(RequestTransaction requestTransaction){
        log("broker",requestTransaction);
        // 4 types of transaction
        // 1. create topic partition -- asynchronous
        // 2. delete topic partition -- asynchronous
        // 3. write producer record into topic partition -- synchronous -> asynchronous operation
        // 4. read consumer record from topic partition  -- asynchronous
        switch (requestTransaction.getRequestType()){
            case TOPIC_PARTITION_CREATE ->{
                final TopicCreateRequestTransaction topicCreateRequestTransaction = (TopicCreateRequestTransaction) requestTransaction;

                Helper.mapArray(topicCreateRequestTransaction.getPartitionNodes(),(i,node)->{
                    if (((Node)node).getId().equals(kafkaProperties.getKafkaProperty("kafka.server.node.id"))){
                        broker.createTopic(topicCreateRequestTransaction.getTopic().getTopicName(),i);
                    }
                });

//                for (int partition : topicCreateRequestTransaction.getPartitions()) {
//                    broker.createTopic(topicCreateRequestTransaction.getTopic().getTopicName(),partition);
//                }
            }case TOPIC_PARTITION_DELETE -> {
                final TopicDeleteRequestTransaction topicDeleteRequestTransaction = (TopicDeleteRequestTransaction) requestTransaction;

                Helper.mapArray(topicDeleteRequestTransaction.getPartitionNodes(),(i,node)->{
                    if (((Node)node).getId().equals(kafkaProperties.getKafkaProperty("kafka.server.node.id"))){
                        broker.removeTopic(topicDeleteRequestTransaction.getTopic().getTopicName(),i);
                    }
                });
//                for (int partition : topicDeleteRequestTransaction.getPartitions()) {
//                    broker.removeTopic(topicDeleteRequestTransaction.getTopic().getTopicName(),partition);
//                }
            }case CONSUMER_RECORD_READ -> {
                final ConsumerRecordReadRequestTransaction consumerRecordReadRequestTransaction = (ConsumerRecordReadRequestTransaction) requestTransaction;
                final org.ekbana.minikafka.common.ConsumerRecords records = broker.getConsumer(consumerRecordReadRequestTransaction.getTopic().getTopicName(), consumerRecordReadRequestTransaction.getPartition())
                        .getRecords(consumerRecordReadRequestTransaction.getOffset(), consumerRecordReadRequestTransaction.isTimeOffset());

                final ConsumerRecords consumerRecords = new ConsumerRecords(consumerRecordReadRequestTransaction.getPartition(),records.count(), records.getStartingOffset(), records.getEndingOffset(), records.stream().map(Record::getData).collect(Collectors.toList()));
                System.out.println(consumerRecords);
                response(new ConsumerRecordReadResponseTransaction(
                        consumerRecordReadRequestTransaction.getTransactionId(),
                        TransactionType.Action.SUCCESS,
                        consumerRecords
                ));
            }case PRODUCER_RECORD_WRITE -> {
                final ProducerRecordWriteRequestTransaction producerRecordWriteRequestTransaction=(ProducerRecordWriteRequestTransaction)requestTransaction;
                producerRecordWriteRequestTransaction.getProducerRecords().forEach(System.out::println);

                final List<Record> collect = producerRecordWriteRequestTransaction.getProducerRecords().stream().map(data -> new Record(producerRecordWriteRequestTransaction.getTopic().getTopicName(), (String) data, producerRecordWriteRequestTransaction.getPartition())).collect(Collectors.toList());
                broker.getProducer(producerRecordWriteRequestTransaction.getTopic().getTopicName(),producerRecordWriteRequestTransaction.getPartition()).addRecords(new ProducerRecords(collect));
                //                broker.getProducer(producerRecordWriteRequestTransaction.getTopic().getTopicName(),producerRecordWriteRequestTransaction.getPartition())
//                        .addRecords(null);
            }
        }
    }
}
