package org.ekbana.server.leader;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.ekbana.server.cluster.Cluster;
import org.ekbana.server.cluster.Node;
import org.ekbana.server.common.cm.request.*;
import org.ekbana.server.common.cm.response.*;
import org.ekbana.server.common.l.LFRequest;
import org.ekbana.server.common.l.LFResponse;
import org.ekbana.server.common.l.LRequest;
import org.ekbana.server.common.l.LResponse;
import org.ekbana.server.common.lr.RBaseTransaction;
import org.ekbana.server.common.lr.RTransaction;
import org.ekbana.server.common.mb.*;
import org.ekbana.server.replica.Replica;
import org.ekbana.server.util.Deserializer;
import org.ekbana.server.util.Mapper;
import org.ekbana.server.util.QueueProcessor;
import org.ekbana.server.util.Serializer;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

public class LeaderController {

    private Cluster cluster=new Cluster();

    private final KafkaServerConfig kafkaServerConfig;
    private final Deserializer deserializer;
    private final Serializer serializer;
//    private final NodeClientMapper nodeClientMapper;
    private final Mapper<String,LeaderClient> nodeClientMapper;
    private final TransactionManager transactionManager;

    private final Mapper<Long, KafkaClientRequestWrapper> transactionClientMapper;

    private final Replica replica;

    private final QueueProcessor<Transaction> transactionRequestProcessor;
    private final QueueProcessor<KafkaClientResponseWrapper> clientResponseProcessor;
    private final QueueProcessor<Object> leaderClientRRProcessor;

    private QueueProcessor.QueueProcessorListener<Object> kafkaLeaderClientQueueProcessor = new QueueProcessor.QueueProcessorListener<Object>() {
        @Override
        public void process(Object o) {

            try {
                if (o instanceof KafkaClientResponseWrapper) {
                    final KafkaClientResponseWrapper o1 = (KafkaClientResponseWrapper) o;
                    print(o1.getLeaderClient().getNode(), o1.getKafkaClientResponse());
                    responseToClient(o1.leaderClient, o1.getKafkaClientResponse());
                } else if (o instanceof KafkaTransactionWrapper) {
                    final KafkaTransactionWrapper o2 = (KafkaTransactionWrapper) o;
                    print(o2.getLeaderClient().getNode(), o2.getTransaction());
                    transactionToNode(o2.getLeaderClient(), o2.getTransaction());
                } else if (o instanceof KafkaRTransactionWrapper) {
                    final KafkaRTransactionWrapper o3 = (KafkaRTransactionWrapper) o;
                    print(o3.getLeaderClient().getNode(), o3.getRTransaction());
                    rTransactionToReplica(o3.leaderClient, o3.getRTransaction());
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    };
    private QueueProcessor.QueueProcessorListener<KafkaClientResponseWrapper> kafkaClientResponseQueueProcessorListener = kafkaClientResponseWrapper -> {
//                try {
////                    responseToClient(kafkaClientResponseWrapper.getLeaderClient(), kafkaClientResponseWrapper.getKafkaClientResponse());
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
    };

    private QueueProcessor.QueueProcessorListener<Transaction> transactionQueueProcessorListener = transaction -> {
        try {
            requestToDataNode(transaction);
        } catch (IOException e) {
            e.printStackTrace();
        }
    };

    public LeaderController(KafkaServerConfig kafkaServerConfig, Deserializer deserializer, Serializer serializer, Mapper<String ,LeaderClient> nodeClientMapper, TransactionManager transactionManager, Replica replica, ExecutorService executorService) {
        this.kafkaServerConfig = kafkaServerConfig;
        this.deserializer = deserializer;
        this.serializer = serializer;
        this.nodeClientMapper = nodeClientMapper;
        this.transactionManager = transactionManager;
        this.replica = replica;

        transactionClientMapper = new Mapper<>();
        transactionRequestProcessor = new QueueProcessor<>(kafkaServerConfig.getQueueSize(), transactionQueueProcessorListener, executorService);
        clientResponseProcessor = new QueueProcessor<>(kafkaServerConfig.getQueueSize(), kafkaClientResponseQueueProcessorListener, executorService);
        leaderClientRRProcessor = new QueueProcessor<>(kafkaServerConfig.getQueueSize(), kafkaLeaderClientQueueProcessor, executorService);
    }

    public void rawData(LeaderClient leaderClient, byte[] data) {
        // deserialize the raw data
        try {
            final Object deserialized = deserializer.deserialize(data);
            print(leaderClient.getNode(), deserialized);
            if (deserialized instanceof LFRequest) {
                processFollowerRequest(leaderClient, (LFRequest) deserialized);
            }
            if (deserialized instanceof LRequest) {
                processRequest(leaderClient, (LRequest) deserialized);
            } else if (deserialized instanceof LResponse) {
                processResponse(leaderClient, (LResponse) deserialized);
            }
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public void print(Node node, Object obj) {
        System.out.println("[Leader] [" + node.getId() + "] " + obj);
    }

    protected void processFollowerRequest(LeaderClient leaderClient, LFRequest lfRequest) {
        if (lfRequest.getLfRequestType() == LFRequest.LFRequestType.NEW) {
            leaderClient.setLeaderClientState(LeaderClientState.CONNECTED);
            try {
                final LFResponse obj = new LFResponse(LFResponse.LFResponseType.CONNECTED);
                print(leaderClient.getNode(), obj);
                leaderClient.send(serializer.serialize(obj));
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else if (lfRequest.getLfRequestType() == LFRequest.LFRequestType.AUTH) {
            try {
                leaderClient.setLeaderClientState(LeaderClientState.AUTHENTICATED);
                final LFResponse obj = new LFResponse(LFResponse.LFResponseType.AUTHENTICATED);
                print(leaderClient.getNode(), obj);
                leaderClient.send(serializer.serialize(obj));
                leaderClient.setNode(new Node(lfRequest.getNodeId(),leaderClient.getNode().getAddress()));
                addNode(leaderClient);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void processRequest(LeaderClient leaderClient, LRequest lRequest) {
        switch (lRequest.getMode()) {
            case MODE_REPLICA -> requestFromReplica(leaderClient, (RTransaction) lRequest.getObject());
            case MODE_DATA -> requestFromDataNode();
            case MODE_CLIENT -> requestFromClient(leaderClient, (KafkaClientRequest) lRequest.getObject());
        }
    }

    public void processResponse(LeaderClient leaderClient, LResponse lResponse) {
        switch (lResponse.getMode()) {
            case MODE_REPLICA -> responseFromReplica(leaderClient, (RTransaction) lResponse.getObject());
            case MODE_DATA -> responseFromData(leaderClient, (Transaction) lResponse.getObject());
            case MODE_CLIENT -> responseFromClient();
        }
    }

    private void responseFromClient() {

    }

    private void responseFromData(LeaderClient leaderClient, Transaction transaction) {
        System.out.println("[Leader Controller] ["+leaderClient.getNode() +"]" + transaction);
        if (transaction.getAction() == TransactionType.Action.ACKNOWLEDGE) {
            // send commit request
            if (transactionManager.hasTransaction(transaction.getTransactionId())) {
                transactionManager.updateTransaction3PhaseStatus(transaction.getTransactionId(), leaderClient.getNode(), ThreePhaseTransactionStatus.ACK);
                if (transactionManager.readyToCommit(transaction.getTransactionId())) {
                    transactionRequestProcessor.push(new CommitRequestTransaction(transaction.getTransactionId()), true);
                    if (transactionManager.hasTransaction(transaction.getTransactionId())) {
                        requestToReplica(leaderClient, (RequestTransaction) transactionManager.getTransaction(transaction.getTransactionId()));
                    }
                }
            }
        } else if (transaction.getAction() == TransactionType.Action.PASS) {
            // send abort request
            transactionRequestProcessor.push(new AbortRequestTransaction(transaction.getTransactionId()), true);
        } else if (transaction.getAction() == TransactionType.Action.SUCCESS) {
            // send response to client
            final RequestTransaction requestTransaction = (RequestTransaction) transactionManager.getTransaction(transaction.getTransactionId());
            final KafkaClientRequestWrapper kafkaClientRequestWrapper = transactionClientMapper.get(requestTransaction.getTransactionId());

            if (requestTransaction.getRequestType() == TransactionType.RequestType.CONSUMER_RECORD_READ) {
                final ConsumerRecordReadResponseTransaction consumerRecordReadResponseTransaction = (ConsumerRecordReadResponseTransaction) transaction;
                System.out.println("[consumer record read response transaction]"+consumerRecordReadResponseTransaction);
                leaderClientRRProcessor.push(
                        new KafkaClientResponseWrapper(
                                new ConsumerRecordResponse(kafkaClientRequestWrapper.getClientRequestId(),
                                        kafkaClientRequestWrapper.getRequestType(),
                                        KafkaClientResponse.ResponseType.SUCCESS,
                                        consumerRecordReadResponseTransaction.getConsumerRecords()), kafkaClientRequestWrapper.getLeaderClient()
                        ), false
                );
            }

            transactionManager.deleteTransaction(transaction.getTransactionId());
            transactionClientMapper.delete(transaction.getTransactionId());

        } else if (transaction.getAction() == TransactionType.Action.FAIL) {
            // send response to client
        }
    }

    private void responseFromReplica(LeaderClient leaderClient, RTransaction rTransaction) {
        // heartbeat and synchronization
        if (rTransaction.getRTransactionType() == RTransaction.RTransactionType.ACKNOWLEDGE) {
            leaderClientRRProcessor.push(new KafkaRTransactionWrapper(new RTransaction(rTransaction.getRTransactionId(), RTransaction.RTransactionType.COMMIT), leaderClient), false);
        }
    }

    public void requestFromReplica(LeaderClient leaderClient, RTransaction rTransaction) {

    }

    public void requestFromDataNode() {

    }

    public void requestFromClient(LeaderClient leaderClient, KafkaClientRequest kafkaClientRequest) {

        switch (kafkaClientRequest.getRequestType()) {
            case PRODUCER_RECORD_WRITE -> {
                // partitioning logic
                // send request to data and replica node
                // convert to producer write transaction
                // register in transaction mapper

                final ProducerRecordWriteRequest producerRecordWriteRequest = (ProducerRecordWriteRequest) kafkaClientRequest;
                if (replica.hasTopic(producerRecordWriteRequest.getTopicName())) {
                    // send to partition logic and get partition id
                    Topic topic = replica.getTopic(producerRecordWriteRequest.getTopicName());
//                    int partitionId = 0;
                    // Topology -> topic, key,partitionId, record_count

                    final ProducerRecordWriteRequestTransaction producerRecordWriteRequestTransaction = new ProducerRecordWriteRequestTransaction(
                            TransactionIdGenerator.getTransactionId(),
                            TransactionType.Action.REGISTER,
                            topic,
                            producerRecordWriteRequest.getPartitionId(),
                            producerRecordWriteRequest.getProducerRecords()
                    );

                    transactionClientMapper.add(producerRecordWriteRequestTransaction.getTransactionId(),
                            new KafkaClientRequestWrapper(kafkaClientRequest.getRequestType(), kafkaClientRequest.getClientRequestId(), leaderClient));
                    transactionRequestProcessor.push(producerRecordWriteRequestTransaction, false);
                } else {
                    leaderClientRRProcessor.push(
                            new KafkaClientResponseWrapper(
                                    new BaseResponse(kafkaClientRequest.getClientRequestId(),
                                            kafkaClientRequest.getRequestType(),
                                            KafkaClientResponse.ResponseType.FAIL,
                                            "topic does not exists"),
                                    leaderClient
                            ), false);
                }

            }
            case CONSUMER_RECORD_READ -> {
                final ConsumerRecordReadRequest consumerRecordReadRequest = (ConsumerRecordReadRequest) kafkaClientRequest;
                if (replica.hasTopic(consumerRecordReadRequest.getTopicName())) {
                    final Topic topic = replica.getTopic(consumerRecordReadRequest.getTopicName());

                    final ConsumerRecordReadRequestTransaction consumerRecordReadRequestTransaction = new ConsumerRecordReadRequestTransaction(
                            TransactionIdGenerator.getTransactionId(),
                            TransactionType.Action.REGISTER,
                            topic,
                            TransactionType.RequestType.CONSUMER_RECORD_READ,
                            consumerRecordReadRequest.getPartitionId(),
                            consumerRecordReadRequest.getOffset(),
                            consumerRecordReadRequest.isTimeOffset()
                    );

                    transactionClientMapper.add(consumerRecordReadRequestTransaction.getTransactionId(),
                            new KafkaClientRequestWrapper(kafkaClientRequest.getRequestType(), kafkaClientRequest.getClientRequestId(), leaderClient));
                    transactionRequestProcessor.push(consumerRecordReadRequestTransaction, false);
                } else {
                    leaderClientRRProcessor.push(
                            new KafkaClientResponseWrapper(
                                    new BaseResponse(kafkaClientRequest.getClientRequestId(),
                                            kafkaClientRequest.getRequestType(),
                                            KafkaClientResponse.ResponseType.FAIL,
                                            "topic does not exists"),
                                    leaderClient
                            ), false);
                }
                // send request to data node
            }
            case CONSUMER_OFFSET_COMMIT -> {
                // send request to replica
            }
            case TOPIC_CREATE -> {
                final TopicCreateRequest topicCreateRequest = (TopicCreateRequest) kafkaClientRequest;
                // validation of topic, partition
                if (replica.hasTopic(topicCreateRequest.getTopicName())) {
                    // throw topic already exists response
                    leaderClientRRProcessor.push(new KafkaClientResponseWrapper(
                            new BaseResponse(
                                    kafkaClientRequest.getClientRequestId(),
                                    kafkaClientRequest.getRequestType(),
                                    KafkaClientResponse.ResponseType.FAIL,
                                    "topic already exists"
                            ), leaderClient), false);
                    return;
                }
                // create a topic
                // select the nodes for topic creation
                // resource manager, number of partition
                final Topic newTopic = Topic.builder()
                        .topicName(((TopicCreateRequest) kafkaClientRequest).getTopicName())
                        .numberOfPartitions(topicCreateRequest.getNumberOfPartitions())
                        .dataNode(cluster.getNodes(topicCreateRequest.getNumberOfPartitions()))
                        .build();
                // resource manager
                // node manager
                // partition policy -
                // send request to each node
                // create create topic request transaction
                final TopicCreateRequestTransaction topicCreateRequestTransaction = new TopicCreateRequestTransaction(TransactionIdGenerator.getTransactionId(),
                        TransactionType.Action.REGISTER,
                        newTopic,
                        TransactionType.RequestType.TOPIC_PARTITION_CREATE);
                transactionClientMapper.add(topicCreateRequestTransaction.getTransactionId(), new KafkaClientRequestWrapper(kafkaClientRequest.getRequestType(), kafkaClientRequest.getClientRequestId(), leaderClient));
                transactionRequestProcessor.push(topicCreateRequestTransaction, false);

            }
            case TOPIC_DELETE -> {
                final TopicDeleteRequest topicDeleteRequest = (TopicDeleteRequest) kafkaClientRequest;
                // topic validation
                if (!replica.hasTopic(topicDeleteRequest.getTopicName())) {
                    // throw topic does not exists response
                    leaderClientRRProcessor.push(new KafkaClientResponseWrapper(
                            new BaseResponse(
                                    kafkaClientRequest.getClientRequestId(),
                                    kafkaClientRequest.getRequestType(),
                                    KafkaClientResponse.ResponseType.FAIL,
                                    "topic does not exists"
                            ), leaderClient), false);
                    return;
                }
                final Topic deleteTopic = replica.getTopic(topicDeleteRequest.getTopicName());
                TopicDeleteRequestTransaction topicDeleteRequestTransaction =
                        new TopicDeleteRequestTransaction(TransactionIdGenerator.getTransactionId(),
                                TransactionType.Action.REGISTER,
                                deleteTopic, TransactionType.RequestType.TOPIC_PARTITION_DELETE);
                // send request to both data node and replica node
                transactionClientMapper.add(topicDeleteRequestTransaction.getTransactionId(), new KafkaClientRequestWrapper(kafkaClientRequest.getRequestType(), kafkaClientRequest.getClientRequestId(), leaderClient));
                transactionRequestProcessor.push(topicDeleteRequestTransaction, false);
            }
            case PRODUCER_CONFIG -> {
                final ProducerConfigRequest producerConfigRequest = (ProducerConfigRequest) kafkaClientRequest;
                // topic and partition validation
                if (!replica.hasTopic(producerConfigRequest.getTopicName())) {
                    // throw topic does not exists response
                    leaderClientRRProcessor.push(new KafkaClientResponseWrapper(
                            new BaseResponse(
                                    kafkaClientRequest.getClientRequestId(),
                                    kafkaClientRequest.getRequestType(),
                                    KafkaClientResponse.ResponseType.FAIL,
                                    "topic does not exists"
                            ), leaderClient), false);
                    return;
                }

                // configuration of consumer group
                //

                leaderClientRRProcessor.push(new KafkaClientResponseWrapper(
                        new ProducerConfigResponse(
                                kafkaClientRequest.getClientRequestId(),
                                replica.getTopic(producerConfigRequest.getTopicName())
                        ), leaderClient), false);

            }
            case CONSUMER_CONFIG -> {
                final ConsumerConfigRequest consumerConfigRequest = (ConsumerConfigRequest) kafkaClientRequest;
                // topic and partition validation
                if (!replica.hasTopic(consumerConfigRequest.getTopicName())) {
                    // throw topic does not exists response
                    leaderClientRRProcessor.push(new KafkaClientResponseWrapper(
                            new BaseResponse(
                                    kafkaClientRequest.getClientRequestId(),
                                    kafkaClientRequest.getRequestType(),
                                    KafkaClientResponse.ResponseType.FAIL,
                                    "topic does not exists"
                            ), leaderClient), false);
                    return;
                }

                // configuration of consumer group
                //
                leaderClientRRProcessor.push(new KafkaClientResponseWrapper(
                        new ConsumerConfigResponse(
                                kafkaClientRequest.getClientRequestId(),
                                replica.getTopic(consumerConfigRequest.getTopicName())
                        ), leaderClient), false);
            }
            default -> {
                // do nothing
            }
        }

    }

    public void requestToDataNode(Transaction transaction) throws IOException {
//        System.out.println("[LEADER-> NODE] "+transaction);

        if (transaction.getAction() == TransactionType.Action.REGISTER) {
            final RequestTransaction requestTransaction = (RequestTransaction) transaction;
            transactionManager.registerTransaction(requestTransaction);

            System.out.println(requestTransaction.getTopic());
            for (Node partitionNode : Arrays.stream(requestTransaction.getPartitionNodes()).collect(Collectors.toSet())) {
//                nodeClientMapper.getLeaderClient(partitionNode).send(serializer.serialize(requestTransaction));
                // manage the 3-phase transaction status
                final LeaderClient leaderClient = nodeClientMapper.get(partitionNode.getId());
                if (leaderClient != null)
                    leaderClientRRProcessor.push(new KafkaTransactionWrapper(requestTransaction, leaderClient), false);
                else {
                    // TODO: handle if node is not alive at moment
                    transactionManager.updateTransaction3PhaseStatus(transaction.getTransactionId(), partitionNode, ThreePhaseTransactionStatus.ACK);
                }
            }
            if (transactionManager.readyToCommit(transaction.getTransactionId())) {
                transactionRequestProcessor.push(new CommitRequestTransaction(transaction.getTransactionId()), false);
                requestToReplica(null, (RequestTransaction) transactionManager.getTransaction(transaction.getTransactionId()));
            }

        } else if (transaction.getAction() == TransactionType.Action.COMMIT) {
            for (Node partitionNode : Arrays.stream(transactionManager.getPartitionNodes(transaction.getTransactionId())).collect(Collectors.toSet())) {
                final LeaderClient leaderClient = nodeClientMapper.get(partitionNode.getId());
                if (leaderClient != null)
                    leaderClientRRProcessor.push(new KafkaTransactionWrapper(transaction, leaderClient), false);
//                nodeClientMapper.getLeaderClient(partitionNode).send(serializer.serialize(transaction));
            }
            // response back to client
            final KafkaClientRequestWrapper kafkaClientRequestWrapper = transactionClientMapper.get(transaction.getTransactionId());

            if (kafkaClientRequestWrapper.getRequestType() != KafkaClientRequest.RequestType.CONSUMER_RECORD_READ) {
                transactionManager.deleteTransaction(transaction.getTransactionId());

                leaderClientRRProcessor.push(new KafkaClientResponseWrapper(
                        new BaseResponse(
                                kafkaClientRequestWrapper.getClientRequestId(),
                                kafkaClientRequestWrapper.getRequestType(),
                                KafkaClientResponse.ResponseType.SUCCESS,
                                switch (kafkaClientRequestWrapper.getRequestType()) {
                                    case CONSUMER_OFFSET_COMMIT -> "offset committed";
                                    case TOPIC_CREATE -> " topic created";
                                    case TOPIC_DELETE -> " topic deleted";
                                    case PRODUCER_RECORD_WRITE -> "producer record written";
                                    default -> "success";
                                }
                        ), kafkaClientRequestWrapper.getLeaderClient()
                ), false);

//                clientResponseProcessor.push(new KafkaClientResponseWrapper(
//                        new BaseResponse(
//                                kafkaClientRequestWrapper.getClientRequestId(),
//                                kafkaClientRequestWrapper.getRequestType(),
//                                KafkaClientResponse.ResponseType.SUCCESS,
//                                "Created topic"
//                        ), kafkaClientRequestWrapper.getLeaderClient()
//                ),false);
            }

        } else if (transaction.getAction() == TransactionType.Action.ABORT) {
            for (Node partitionNode : transactionManager.getPartitionNodes(transaction.getTransactionId())) {
//                nodeClientMapper.getLeaderClient(partitionNode).send(serializer.serialize(transaction));
                final LeaderClient leaderClient = nodeClientMapper.get(partitionNode.getId());
                if (leaderClient != null)
                    leaderClientRRProcessor.push(new KafkaTransactionWrapper(transaction, leaderClient), false);
            }

            // response back to client
            final KafkaClientRequestWrapper kafkaClientRequestWrapper = transactionClientMapper.get(transaction.getTransactionId());

            leaderClientRRProcessor.push(new KafkaClientResponseWrapper(
                    new BaseResponse(
                            kafkaClientRequestWrapper.getClientRequestId(),
                            kafkaClientRequestWrapper.getRequestType(),
                            KafkaClientResponse.ResponseType.FAIL,
                            "Fail"
                    ), kafkaClientRequestWrapper.getLeaderClient()
            ), false);

//            clientResponseProcessor.push(new KafkaClientResponseWrapper(
//                    new BaseResponse(
//                            kafkaClientRequestWrapper.getClientRequestId(),
//                            kafkaClientRequestWrapper.getRequestType(),
//                            KafkaClientResponse.ResponseType.FAIL,
//                            "Fail"
//                    ), kafkaClientRequestWrapper.getLeaderClient()
//            ),false);

            transactionManager.deleteTransaction(transaction.getTransactionId());
        }
    }

    public void requestToAllReplica(RBaseTransaction rBaseTransaction) {
        nodeClientMapper.forEach((node, leaderClient) -> {
            leaderClientRRProcessor.push(new KafkaRTransactionWrapper(rBaseTransaction, leaderClient), false);
        });
    }

    public void requestToReplica(LeaderClient leaderClient, RequestTransaction requestTransaction) {
        if (requestTransaction.getRequestType() == TransactionType.RequestType.TOPIC_PARTITION_CREATE) {
            final RBaseTransaction rTransaction = new RBaseTransaction(TransactionIdGenerator.getrTransactionId(), RTransaction.RTransactionType.SYNC, RTransaction.RRequestType.CREATE_TOPIC, requestTransaction.getTopic());
            requestToAllReplica(rTransaction);
//            leaderClientRRProcessor.push(new KafkaRTransactionWrapper(rTransaction,leaderClient),false);
        } else if (requestTransaction.getRequestType() == TransactionType.RequestType.TOPIC_PARTITION_DELETE) {
            final RBaseTransaction rTransaction = new RBaseTransaction(TransactionIdGenerator.getrTransactionId(), RTransaction.RTransactionType.SYNC, RTransaction.RRequestType.DELETE_TOPIC, requestTransaction.getTopic());
            requestToAllReplica(rTransaction);
//            leaderClientRRProcessor.push(new KafkaRTransactionWrapper(rTransaction,leaderClient),false);
        }
    }

    public void requestToClient() {

    }

    public void responseToReplica() {

    }

    public void responseToDataNode() {

    }

    public void responseToClient(LeaderClient leaderClient, KafkaClientResponse kafkaClientResponse) throws IOException {
        if (leaderClient != null) leaderClient.send(serializer.serialize(kafkaClientResponse));
    }

    public void transactionToNode(LeaderClient leaderClient, Transaction transaction) throws IOException {
        leaderClient.send(serializer.serialize(transaction));
    }

    public void rTransactionToReplica(LeaderClient leaderClient, RTransaction rTransaction) throws IOException {
        leaderClient.send(serializer.serialize(rTransaction));
    }

    public void addNode(LeaderClient leaderClient) {
        System.out.println("[Leader Controller] new node  : " + leaderClient.getNode());
        nodeClientMapper.add(leaderClient.getNode().getId(), leaderClient);
        cluster.addNode(leaderClient.getNode());
    }

    public void removeNode(LeaderClient leaderClient) {
        System.out.println("[Leader Controller] removing node : " + leaderClient.getNode());
        nodeClientMapper.delete(leaderClient.getNode().getId());
        cluster.deleteNode(leaderClient.getNode());
    }

    @Getter
    @AllArgsConstructor
    private static class KafkaClientResponseWrapper {
        private final KafkaClientResponse kafkaClientResponse;
        private final LeaderClient leaderClient;
    }

    @Getter
    @AllArgsConstructor
    private static class KafkaTransactionWrapper {
        private final Transaction transaction;
        private final LeaderClient leaderClient;
    }

    @Getter
    @AllArgsConstructor
    private static class KafkaRTransactionWrapper {
        private final RTransaction rTransaction;
        private final LeaderClient leaderClient;
    }

    @Getter
    @AllArgsConstructor
    private static class KafkaClientRequestWrapper {
        private final KafkaClientRequest.RequestType requestType;
        private final long clientRequestId;
        private final LeaderClient leaderClient;
    }

    private static class TransactionIdGenerator {
        private static long transactionId = 0;
        private static long rTransactionId = 0;

        public static long getTransactionId() {
            transactionId = transactionId + 1;
            return transactionId;
        }

        public static long getrTransactionId() {
            rTransactionId = rTransactionId + 1;
            return rTransactionId;
        }


    }
}
