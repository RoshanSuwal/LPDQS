package org.ekbana.server.leader;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.ekbana.server.common.cm.request.*;
import org.ekbana.server.common.cm.response.BaseResponse;
import org.ekbana.server.common.cm.response.KafkaClientResponse;
import org.ekbana.server.common.l.LRequest;
import org.ekbana.server.common.l.LResponse;
import org.ekbana.server.common.mb.*;
import org.ekbana.server.util.Deserializer;
import org.ekbana.server.util.Mapper;
import org.ekbana.server.util.QueueProcessor;
import org.ekbana.server.util.Serializer;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

public class LeaderController {

    private final KafkaServerConfig kafkaServerConfig;
    private final Deserializer deserializer;
    private final Serializer serializer;
    private final NodeClientMapper nodeClientMapper;
    private final TransactionManager transactionManager;

    private final Mapper<Long,KafkaClientRequestWrapper> transactionClientMapper;

    private final Mapper<String, Topic> topicMapper;

    private final QueueProcessor<Transaction> transactionRequestProcessor;
    private final QueueProcessor<KafkaClientResponseWrapper> clientResponseProcessor;

    private QueueProcessor.QueueProcessorListener<KafkaClientResponseWrapper> kafkaClientResponseQueueProcessorListener= kafkaClientResponseWrapper -> {
                try {
                    responseToClient(kafkaClientResponseWrapper.getLeaderClient(), kafkaClientResponseWrapper.getKafkaClientResponse());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            };

    private  QueueProcessor.QueueProcessorListener<Transaction> transactionQueueProcessorListener= transaction -> {
        try {
            requestToDataNode(transaction);
        } catch (IOException e) {
            e.printStackTrace();
        }
    };

    public LeaderController(KafkaServerConfig kafkaServerConfig, Deserializer deserializer, Serializer serializer, NodeClientMapper nodeClientMapper, TransactionManager transactionManager, Mapper<String, Topic> topicMapper, ExecutorService executorService) {
        this.kafkaServerConfig = kafkaServerConfig;
        this.deserializer = deserializer;
        this.serializer = serializer;
        this.nodeClientMapper = nodeClientMapper;
        this.transactionManager = transactionManager;
        this.topicMapper = topicMapper;

        transactionClientMapper=new Mapper<>();
        transactionRequestProcessor=new QueueProcessor<>(kafkaServerConfig.getQueueSize(),transactionQueueProcessorListener,executorService);
        clientResponseProcessor=new QueueProcessor<>(kafkaServerConfig.getQueueSize(),kafkaClientResponseQueueProcessorListener,executorService);
    }

    public void rawData(LeaderClient leaderClient, byte[] data){
        // deserialize the raw data
        try {
            final Object deserialized = deserializer.deserialize(data);
            if (deserialized instanceof LRequest){
                processRequest(leaderClient,(LRequest) deserialized);
            }else if (deserialized instanceof LResponse){
                processResponse(leaderClient,(LResponse) deserialized);
            }
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public void processRequest(LeaderClient leaderClient,LRequest lRequest){
        switch (lRequest.getMode()){
            case MODE_REPLICA -> requestFromReplica();
            case MODE_DATA -> requestFromDataNode();
            case MODE_CLIENT -> requestFromClient(leaderClient,(KafkaClientRequest) lRequest.getObject());
        }
    }

    public void processResponse(LeaderClient leaderClient,LResponse lResponse){
        switch (lResponse.getMode()){
            case MODE_REPLICA -> responseFromReplica();
            case MODE_DATA -> responseFromData(leaderClient,(Transaction) lResponse.getObject());
            case MODE_CLIENT -> responseFromClient();
        }
    }

    private void responseFromClient() {

    }

    private void responseFromData(LeaderClient leaderClient, Transaction transaction) {
        System.out.println("[DATA->LEADER] "+transaction);
        if (transaction.getAction()== TransactionType.Action.ACKNOWLEDGE){
            // send commit request
            if (transactionManager.hasTransaction(transaction.getTransactionId())) {
                transactionManager.updateTransaction3PhaseStatus(transaction.getTransactionId(), leaderClient.getNode(), ThreePhaseTransactionStatus.ACK);
                if (transactionManager.readyToCommit(transaction.getTransactionId())) {
                    transactionRequestProcessor.push(new CommitRequestTransaction(transaction.getTransactionId()), true);
                }
            }
        }else if (transaction.getAction()== TransactionType.Action.PASS){
            // send abort request
            transactionRequestProcessor.push(new AbortRequestTransaction(transaction.getTransactionId()),true);
        }else if (transaction.getAction()== TransactionType.Action.SUCCESS){
            // send response to client


        }else if (transaction.getAction()== TransactionType.Action.FAIL){
            // send response to client
        }
    }

    private void responseFromReplica() {
        // heartbeat and synchronization
    }

    public void requestFromReplica(){

    }

    public void requestFromDataNode(){

    }

    public void requestFromClient(LeaderClient leaderClient, KafkaClientRequest kafkaClientRequest){

        System.out.println("[CLIENT->LEADER] "+kafkaClientRequest);

        switch (kafkaClientRequest.getRequestType()){
            case PRODUCER_RECORD_WRITE -> {
                // send request to data and replica node
                // convert to producer write transaction
                // register in transaction mapper
            }case CONSUMER_RECORD_READ -> {
                // send request to data node
            }case CONSUMER_OFFSET_COMMIT -> {
                // send request to replica
            }case TOPIC_CREATE -> {
                final TopicCreateRequest topicCreateRequest = (TopicCreateRequest) kafkaClientRequest;
                // validation of topic, partition
                if (topicMapper.has(topicCreateRequest.getTopicName())){
                    // throw topic already exists response
                    clientResponseProcessor.push(new KafkaClientResponseWrapper(
                            new BaseResponse(
                                kafkaClientRequest.getClientRequestId(),
                                    kafkaClientRequest.getRequestType(),
                                    KafkaClientResponse.ResponseType.FAIL,
                                    "topic already exists"
                    ),leaderClient),false);
                    return;
                }
                // create a topic
                // select the nodes for topic creation
                final Topic newTopic = Topic.builder()
                        .topicName(((TopicCreateRequest) kafkaClientRequest).getTopicName())
                        .numberOfPartitions(1)
                        .dataNode(new Node[]{nodeClientMapper.getAnyNode()})
                        .build();
                // send request to each node
                // create create topic request transaction
                final TopicCreateRequestTransaction topicCreateRequestTransaction = new TopicCreateRequestTransaction(TransactionIdGenerator.getTransactionId(),
                        TransactionType.Action.REGISTER,
                        newTopic,
                        TransactionType.RequestType.TOPIC_PARTITION_CREATE);
                transactionClientMapper.add(topicCreateRequestTransaction.getTransactionId(),new KafkaClientRequestWrapper(kafkaClientRequest.getRequestType(),kafkaClientRequest.getClientRequestId(),leaderClient));
                transactionRequestProcessor.push(topicCreateRequestTransaction,false);

            }case TOPIC_DELETE -> {
                final TopicDeleteRequest topicDeleteRequest = (TopicDeleteRequest) kafkaClientRequest;
                // topic validation
                if (!topicMapper.has(topicDeleteRequest.getTopicName())){
                    // throw topic does not exists response
                    clientResponseProcessor.push(new KafkaClientResponseWrapper(
                            new BaseResponse(
                                    kafkaClientRequest.getClientRequestId(),
                                    kafkaClientRequest.getRequestType(),
                                    KafkaClientResponse.ResponseType.FAIL,
                                    "topic does not exists"
                            ),leaderClient),false);
                    return;
                }
                final Topic deleteTopic = topicMapper.get(topicDeleteRequest.getTopicName());
                TopicDeleteRequestTransaction topicDeleteRequestTransaction=
                        new TopicDeleteRequestTransaction(TransactionIdGenerator.getTransactionId(),
                                TransactionType.Action.REGISTER,
                                deleteTopic, TransactionType.RequestType.TOPIC_PARTITION_DELETE);
                // send request to both data node and replica node
                transactionClientMapper.add(topicDeleteRequestTransaction.getTransactionId(),new KafkaClientRequestWrapper(kafkaClientRequest.getRequestType(),kafkaClientRequest.getClientRequestId(),leaderClient));
                transactionRequestProcessor.push(topicDeleteRequestTransaction,false);
            }case PRODUCER_CONFIG -> {
                final ProducerConfigRequest producerConfigRequest = (ProducerConfigRequest) kafkaClientRequest;
                // topic and partition validation
                if (!topicMapper.has(producerConfigRequest.getTopicName())){
                    // throw topic does not exists response
                    clientResponseProcessor.push(new KafkaClientResponseWrapper(
                            new BaseResponse(
                                    kafkaClientRequest.getClientRequestId(),
                                    kafkaClientRequest.getRequestType(),
                                    KafkaClientResponse.ResponseType.FAIL,
                                    "topic does not exists"
                            ),leaderClient),false);
                    return;
                }

                // configuration of consumer group
                //

                clientResponseProcessor.push(new KafkaClientResponseWrapper(
                        new BaseResponse(
                                kafkaClientRequest.getClientRequestId(),
                                kafkaClientRequest.getRequestType(),
                                KafkaClientResponse.ResponseType.SUCCESS,
                                "producer configured"
                        ),leaderClient),false);

            }case CONSUMER_CONFIG -> {
                final ConsumerConfigRequest consumerConfigRequest = (ConsumerConfigRequest) kafkaClientRequest;
                // topic and partition validation
                if (!topicMapper.has(consumerConfigRequest.getTopicName())){
                    // throw topic does not exists response
                    clientResponseProcessor.push(new KafkaClientResponseWrapper(
                            new BaseResponse(
                                    kafkaClientRequest.getClientRequestId(),
                                    kafkaClientRequest.getRequestType(),
                                    KafkaClientResponse.ResponseType.FAIL,
                                    "topic does not exists"
                            ),leaderClient),false);
                    return;
                }

                // configuration of consumer group
                //

                clientResponseProcessor.push(new KafkaClientResponseWrapper(
                        new BaseResponse(
                                kafkaClientRequest.getClientRequestId(),
                                kafkaClientRequest.getRequestType(),
                                KafkaClientResponse.ResponseType.SUCCESS,
                                "consumer configured"
                        ),leaderClient),false);
            }default -> {
                // do nothing
            }
        }

    }

    public void requestToDataNode(Transaction transaction) throws IOException {
        System.out.println("[LEADER-> NODE] "+transaction);

        if (transaction.getAction()== TransactionType.Action.REGISTER){
            final RequestTransaction requestTransaction = (RequestTransaction) transaction;
            transactionManager.registerTransaction(requestTransaction);

            for (Node partitionNode : requestTransaction.getPartitionNodes()) {
                nodeClientMapper.getLeaderClient(partitionNode).send(serializer.serialize(requestTransaction));
                // manage the 3-phase transaction status
            }
        }else if (transaction.getAction()== TransactionType.Action.COMMIT){
            for (Node partitionNode : transactionManager.getPartitionNodes(transaction.getTransactionId())) {
                nodeClientMapper.getLeaderClient(partitionNode).send(serializer.serialize(transaction));
            }
            // response back to client
            final KafkaClientRequestWrapper kafkaClientRequestWrapper = transactionClientMapper.get(transaction.getTransactionId());

            if (kafkaClientRequestWrapper.getRequestType()!= KafkaClientRequest.RequestType.CONSUMER_RECORD_READ) {
                transactionManager.deleteTransaction(transaction.getTransactionId());

                clientResponseProcessor.push(new KafkaClientResponseWrapper(
                        new BaseResponse(
                                kafkaClientRequestWrapper.getClientRequestId(),
                                kafkaClientRequestWrapper.getRequestType(),
                                KafkaClientResponse.ResponseType.SUCCESS,
                                "Created topic"
                        ), kafkaClientRequestWrapper.getLeaderClient()
                ),false);
            }

        }else if (transaction.getAction()== TransactionType.Action.ABORT){
            for (Node partitionNode : transactionManager.getPartitionNodes(transaction.getTransactionId())) {
                nodeClientMapper.getLeaderClient(partitionNode).send(serializer.serialize(transaction));
            }

            // response back to client
            final KafkaClientRequestWrapper kafkaClientRequestWrapper = transactionClientMapper.get(transaction.getTransactionId());

            clientResponseProcessor.push(new KafkaClientResponseWrapper(
                    new BaseResponse(
                            kafkaClientRequestWrapper.getClientRequestId(),
                            kafkaClientRequestWrapper.getRequestType(),
                            KafkaClientResponse.ResponseType.FAIL,
                            "Fail"
                    ), kafkaClientRequestWrapper.getLeaderClient()
            ),false);
            transactionManager.deleteTransaction(transaction.getTransactionId());
        }
    }

    public void requestToReplica(){

    }

    public void requestToClient(){

    }

    public void responseToReplica(){

    }

    public void responseToDataNode(){

    }

    public void responseToClient(LeaderClient leaderClient,KafkaClientResponse kafkaClientResponse) throws IOException {
        System.out.println("[LEADER->CLIENT] "+kafkaClientResponse);
        leaderClient.send(serializer.serialize(kafkaClientResponse));
    }

    public void addNode(LeaderClient leaderClient){
        System.out.println("Adding the node to nodeMapper : "+leaderClient.getNode().getAddress());
        nodeClientMapper.addNode(leaderClient.getNode(),leaderClient);
    }

    @Getter
    @AllArgsConstructor
    private static class KafkaClientResponseWrapper{
        private final KafkaClientResponse kafkaClientResponse;
        private final LeaderClient leaderClient;
    }

    @Getter
    @AllArgsConstructor
    private static class KafkaClientRequestWrapper{
        private final KafkaClientRequest.RequestType requestType;
        private final long clientRequestId;
        private final LeaderClient leaderClient;
    }

    private static class TransactionIdGenerator{
        private static long transactionId=0;

        public static long getTransactionId(){
            transactionId=transactionId+1;
            return transactionId;
        }
    }
}
