package org.ekbana.server.v2.datanode;

import org.ekbana.broker.Broker;
import org.ekbana.broker.consumer.Consumer;
import org.ekbana.minikafka.common.Node;
import org.ekbana.minikafka.common.Record;
import org.ekbana.server.common.l.LFRequest;
import org.ekbana.server.common.l.LFResponse;
import org.ekbana.server.common.mb.*;
import org.ekbana.server.config.KafkaProperties;
import org.ekbana.server.util.*;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

public class DataNodeController {

    public interface Receiver<T> {
        void onReceive(T t);
    }

    private Serializer serializer;
    private Deserializer deserializer;

    private Broker broker;
    private KafkaProperties kafkaProperties;
    private DataNodeServerClient dataNodeServerClient;

    Receiver<byte[]> receiver = new Receiver<>() {
        @Override
        public void onReceive(byte[] bytes) {
            try {
                final Object deserialize = deserializer.deserialize(bytes);
                if (deserialize instanceof Transaction transaction) {
                    processTransaction(transaction);
                } else if (deserialize instanceof LFResponse lfResponse) {
                    processNode(lfResponse);
                }
            } catch (IOException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
        }
    };

    private Mapper<Long, RequestTransaction> transactionMapper;
    private QueueProcessor<RequestTransaction> requestTransactionQueueProcessor;

    private QueueProcessor<Object> transactionResponseQueueProcessor;

    public DataNodeController(Serializer serializer, Deserializer deserializer, Broker broker, KafkaProperties kafkaProperties, DataNodeServerClient dataNodeServerClient, ExecutorService executorService) {
        this.serializer = serializer;
        this.deserializer = deserializer;
        this.broker = broker;
        this.kafkaProperties = kafkaProperties;
        this.dataNodeServerClient = dataNodeServerClient;
        dataNodeServerClient.setReceiver(receiver);
        transactionMapper = new Mapper<>();
        requestTransactionQueueProcessor = new QueueProcessor<>(100, this::executeTransaction, executorService);
        transactionResponseQueueProcessor = new QueueProcessor<>(100, this::sendToLeader, executorService);
    }

    public void processTransaction(Transaction transaction) {
        KafkaLogger.transactionLogger.info("{} : {}","processing",transaction);
        switch (transaction.getAction()) {
            case REGISTER -> {
                transactionMapper.add(transaction.getTransactionId(), (RequestTransaction) transaction);
                transactionResponseQueueProcessor.push(new BaseResponseTransaction(transaction.getTransactionId(), TransactionType.Action.ACKNOWLEDGE), false);
            }
            case COMMIT -> {
                // register in queue to process transaction
                requestTransactionQueueProcessor.push(transactionMapper.get(transaction.getTransactionId()), false);
            }
            case ABORT -> {
                transactionMapper.delete(transaction.getTransactionId());
            }
            default -> {

            }
        }
    }

    public void processNode(LFResponse lfResponse) {
        KafkaLogger.dataNodeLogger.info("{} : {}","Configuring node",lfResponse);
        if (lfResponse.getLfResponseType() == LFResponse.LFResponseType.CONNECTED) {
            dataNodeServerClient.setNodeState(DataNodeServerClient.NodeState.CONNECTED);
            transactionResponseQueueProcessor.push(new LFRequest(kafkaProperties.getKafkaProperty("kafka.server.node.id"), LFRequest.LFRequestType.CONFIGURE),true);
        } else if (lfResponse.getLfResponseType() == LFResponse.LFResponseType.CONFIGURED) {
            dataNodeServerClient.setNodeState(DataNodeServerClient.NodeState.CONFIGURED);
        }else {
            dataNodeServerClient.setNodeState(DataNodeServerClient.NodeState.CLOSED);
            dataNodeServerClient.close();
        }
    }

    public void sendToLeader(Object obj) {
        KafkaLogger.nodeLogger.info("[Response to leader] {}",obj);
        try {
            if (obj instanceof LFRequest || dataNodeServerClient.getNodeState()== DataNodeServerClient.NodeState.CONFIGURED) {
                dataNodeServerClient.write(serializer.serialize(obj));
                Thread.sleep(100);
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void executeTransaction(RequestTransaction requestTransaction) {
        KafkaLogger.transactionLogger.info("{} : {}","executing",requestTransaction);
        switch (requestTransaction.getRequestType()) {
            case TOPIC_PARTITION_CREATE -> {
                final TopicCreateRequestTransaction topicCreateRequestTransaction = (TopicCreateRequestTransaction) requestTransaction;

                Helper.mapArray(topicCreateRequestTransaction.getPartitionNodes(), (i, node) -> {
                    if (((Node) node).getId().equals(kafkaProperties.getKafkaProperty("kafka.server.node.id"))) {
                        broker.createTopic(topicCreateRequestTransaction.getTopic().getTopicName(), i);
                    }
                });
            }
            case TOPIC_PARTITION_DELETE -> {
                final TopicDeleteRequestTransaction topicDeleteRequestTransaction = (TopicDeleteRequestTransaction) requestTransaction;

                Helper.mapArray(topicDeleteRequestTransaction.getPartitionNodes(), (i, node) -> {
                    if (((Node) node).getId().equals(kafkaProperties.getKafkaProperty("kafka.server.node.id"))) {
                        broker.removeTopic(topicDeleteRequestTransaction.getTopic().getTopicName(), i);
                    }
                });
            }
            case PRODUCER_RECORD_WRITE -> {
                final ProducerRecordWriteRequestTransaction producerRecordWriteRequestTransaction = (ProducerRecordWriteRequestTransaction) requestTransaction;
                final List<Record> collect = producerRecordWriteRequestTransaction.getProducerRecords().stream().map(data -> new Record(producerRecordWriteRequestTransaction.getTopic().getTopicName(), (String) data, producerRecordWriteRequestTransaction.getPartition())).collect(Collectors.toList());
                broker.getProducer(producerRecordWriteRequestTransaction.getTopic().getTopicName(), producerRecordWriteRequestTransaction.getPartition()).addRecords(new org.ekbana.minikafka.common.ProducerRecords(collect));
            }
            case CONSUMER_RECORD_READ -> {
                final ConsumerRecordReadRequestTransaction consumerRecordReadRequestTransaction = (ConsumerRecordReadRequestTransaction) requestTransaction;
                final Consumer consumer = broker.getConsumer(consumerRecordReadRequestTransaction.getTopic().getTopicName(), consumerRecordReadRequestTransaction.getPartition());
                if (consumer==null){
                    transactionResponseQueueProcessor.push(new ConsumerRecordReadResponseTransaction(
                            consumerRecordReadRequestTransaction.getTransactionId(),
                            TransactionType.Action.SUCCESS,
                            null
                    ), false);
                }else {
                    final org.ekbana.minikafka.common.ConsumerRecords records = consumer
                            .getRecords(consumerRecordReadRequestTransaction.getOffset(), consumerRecordReadRequestTransaction.isTimeOffset());

                    final ConsumerRecords consumerRecords = new ConsumerRecords(consumerRecordReadRequestTransaction.getPartition(), records.count(), records.getStartingOffset(), records.getEndingOffset(), records.stream().map(Record::getData).collect(Collectors.toList()));
//                System.out.println(consumerRecords);
                    transactionResponseQueueProcessor.push(new ConsumerRecordReadResponseTransaction(
                            consumerRecordReadRequestTransaction.getTransactionId(),
                            TransactionType.Action.SUCCESS,
                            consumerRecords
                    ), false);
                }
            }
            default -> {

            }
        }
    }
}
