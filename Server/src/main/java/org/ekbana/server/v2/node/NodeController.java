package org.ekbana.server.v2.node;

import org.ekbana.minikafka.common.LBRequest;
import org.ekbana.minikafka.common.Node;
import org.ekbana.server.common.cm.request.ConsumerRecordReadRequest;
import org.ekbana.server.common.cm.request.KafkaClientRequest;
import org.ekbana.server.common.cm.request.ProducerRecordWriteRequest;
import org.ekbana.server.common.cm.response.BaseResponse;
import org.ekbana.server.common.cm.response.ConsumerRecordResponse;
import org.ekbana.server.common.cm.response.KafkaClientResponse;
import org.ekbana.server.common.l.LFRequest;
import org.ekbana.server.common.l.LFResponse;
import org.ekbana.server.common.mb.*;
import org.ekbana.server.leader.LeaderClient;
import org.ekbana.server.leader.LeaderClientState;
import org.ekbana.server.util.*;
import org.ekbana.server.v2.common.Router;
import org.ekbana.server.v2.leader.TopicController;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

public class NodeController {
    // manages the nodes
    // get and put data from nodes
    private TransactionManager transactionManager;
    private Mapper<String, LeaderClient> nodeMapper;
    private Router.NodeRouter router;

    private Mapper<Long, Long> nodeTransactionAndClientRequestMapper;
    private QueueProcessor<Transaction> transactionQueueProcessor;

    private Serializer serializer;
    private Deserializer deserializer;

    private NodeManager nodeManager;
    private TopicController topicController;

    public NodeController(TransactionManager transactionManager, Router.NodeRouter router, Serializer serializer, Deserializer deserializer, ExecutorService executorService, NodeManager nodeManager, TopicController topicController) {
        this.transactionManager = transactionManager;
        this.nodeMapper = new Mapper<>();
        this.router = router;
        this.nodeTransactionAndClientRequestMapper = new Mapper<>();
        this.transactionQueueProcessor = new QueueProcessor<>(100, this::processTransaction, executorService);
        this.serializer = serializer;
        this.deserializer = deserializer;
        this.nodeManager = nodeManager;
        this.topicController = topicController;

    }

    public void processRequest(KafkaClientRequest kafkaClientRequest, Topic topic) {
        System.out.println(kafkaClientRequest + "\n" + topic);
        final Long generateTransactionId = Generator.generateTransactionId();
        switch (kafkaClientRequest.getRequestType()) {
            case TOPIC_CREATE -> {
                registerTransaction(new TopicCreateRequestTransaction(
                        generateTransactionId,
                        TransactionType.Action.REGISTER,
                        topic,
                        TransactionType.RequestType.TOPIC_PARTITION_CREATE
                ));
            }
            case TOPIC_DELETE -> {
                registerTransaction(new TopicDeleteRequestTransaction(
                        generateTransactionId,
                        TransactionType.Action.REGISTER,
                        topic,
                        TransactionType.RequestType.TOPIC_PARTITION_DELETE
                ));
            }
            case PRODUCER_RECORD_WRITE -> {
                registerTransaction(new ProducerRecordWriteRequestTransaction(
                        generateTransactionId,
                        TransactionType.Action.REGISTER,
                        topic,
                        topicController.getPartitionId(topic, LBRequest.builder()
                                .key(((ProducerRecordWriteRequest) kafkaClientRequest).getKey())
                                .id(((ProducerRecordWriteRequest) kafkaClientRequest).getPartitionId())
                                .build()),
//                        ((ProducerRecordWriteRequest) kafkaClientRequest).getPartitionId(),
                        // use the logic for partitioning
                        ((ProducerRecordWriteRequest) kafkaClientRequest).getProducerRecords()
                ));
            }
            case CONSUMER_RECORD_READ -> {
                registerTransaction(new ConsumerRecordReadRequestTransaction(
                        generateTransactionId,
                        TransactionType.Action.REGISTER,
                        topic,
                        TransactionType.RequestType.CONSUMER_RECORD_READ,
                        ((ConsumerRecordReadRequest) kafkaClientRequest).getPartitionId(),
                        ((ConsumerRecordReadRequest) kafkaClientRequest).getOffset(),
                        ((ConsumerRecordReadRequest) kafkaClientRequest).isTimeOffset()
                ));
            }
            default -> {

            }
        }
        nodeTransactionAndClientRequestMapper.add(generateTransactionId, kafkaClientRequest.getClientRequestId());

    }

    public void registerTransaction(Transaction transaction) {
        System.out.println(transaction);
        transactionQueueProcessor.push(transaction, false);
    }

    public void processTransaction(Transaction transaction) {
        System.out.println("process : " + transaction);
        switch (transaction.getAction()) {
            case REGISTER -> {
//                if (((RequestTransaction) transaction).getRequestType() == TransactionType.RequestType.PRODUCER_RECORD_WRITE) {
//                    final ProducerRecordWriteRequestTransaction producerRecordWriteRequestTransaction = (ProducerRecordWriteRequestTransaction) transaction;
//                    final Node node = topicController.getNode(((RequestTransaction) transaction).getTopic(),
//                            LBRequest.builder().requestWeight(((ProducerRecordWriteRequestTransaction) transaction).getProducerRecords().size()).build());
//                    if (node != null) {
//                        producerRecordWriteRequestTransaction.setPartitionNode(node);
//                        transactionManager.registerTransaction(producerRecordWriteRequestTransaction);
//                        sendToNode(nodeMapper.get(node.getId()), producerRecordWriteRequestTransaction);
//                    } else {
//                        transactionManager.deleteTransaction(transaction.getTransactionId());
//                        router.routeToClient(
//                                new BaseResponse(
//                                        nodeTransactionAndClientRequestMapper.get(transaction.getTransactionId()),
//                                        convertTransactionTypeToRequestType(((RequestTransaction) transaction).getRequestType()),
//                                        KafkaClientResponse.ResponseType.FAIL,
//                                        "Node not available"
//                                )
//                        );
//                    }
//
//                } else {

                if (((RequestTransaction) transaction).getPartitionNodes() == null) {
                    router.routeToClient(
                            new BaseResponse(
                                    nodeTransactionAndClientRequestMapper.get(transaction.getTransactionId()),
                                    convertTransactionTypeToRequestType(((RequestTransaction) transaction).getRequestType()),
                                    KafkaClientResponse.ResponseType.FAIL,
                                    "Node not available"
                            )
                    );
                } else {
                    transactionManager.registerTransaction((RequestTransaction) transaction);
                    Arrays.stream(((RequestTransaction) transaction).getPartitionNodes()).collect(Collectors.toSet()).forEach(
                            node -> {
                                sendToNode(nodeMapper.get(node.getId()), transaction);
                            }
                    );
                }
            }
            case COMMIT -> {
                final RequestTransaction managerTransaction = (RequestTransaction) transactionManager.getTransaction(transaction.getTransactionId());
                Arrays.stream(managerTransaction.getPartitionNodes()).collect(Collectors.toSet()).forEach(
                        node -> {
                            sendToNode(nodeMapper.get(node.getId()), transaction);
                        }
                );

                if (managerTransaction.getRequestType() != TransactionType.RequestType.CONSUMER_RECORD_READ) {
                    router.routeToClient(
                            new BaseResponse(
                                    nodeTransactionAndClientRequestMapper.get(transaction.getTransactionId()),
                                    convertTransactionTypeToRequestType(managerTransaction.getRequestType()),
                                    KafkaClientResponse.ResponseType.SUCCESS,
                                    switch (managerTransaction.getRequestType()) {
                                        case TOPIC_CREATE -> "topic created";
                                        case TOPIC_DELETE -> "topic deleted";
                                        case PRODUCER_RECORD_WRITE -> "producer record written";
                                        default -> "success";
                                    }
                            )
                    );

                    if (managerTransaction.getRequestType() == TransactionType.RequestType.TOPIC_PARTITION_CREATE) {
                        topicController.createTopic(managerTransaction.getTopic());
                    } else if (managerTransaction.getRequestType() == TransactionType.RequestType.TOPIC_PARTITION_DELETE) {
                        topicController.removeTopic(managerTransaction.getTopic().getTopicName());
                    }

                    transactionManager.deleteTransaction(transaction.getTransactionId());
                    nodeTransactionAndClientRequestMapper.delete(transaction.getTransactionId());
                }
            }
            case ABORT -> {
                Arrays.stream(((RequestTransaction) transaction).getPartitionNodes()).collect(Collectors.toSet()).forEach(
                        node -> {
                            sendToNode(nodeMapper.get(node.getId()), transaction);
                        }
                );
                // send response to leader
                router.routeToClient(
                        new BaseResponse(
                                nodeTransactionAndClientRequestMapper.get(transaction.getTransactionId()),
                                convertTransactionTypeToRequestType(((RequestTransaction) transaction).getRequestType()),
                                KafkaClientResponse.ResponseType.FAIL,
                                "Fail"
                        )
                );
                transactionManager.deleteTransaction(transaction.getTransactionId());
                nodeTransactionAndClientRequestMapper.delete(transaction.getTransactionId());
            }
            default -> {

            }
        }
    }

    public void processTransactionFromNode(LeaderClient leaderClient, byte[] bytes) {
        try {
            final Object deserialize = deserializer.deserialize(bytes);

            if (deserialize instanceof LFRequest lfRequest) {
                processNodeConfiguration(leaderClient, lfRequest);
            } else if (deserialize instanceof Transaction transaction) {
                // process transaction responses
                processTransactionResponse(leaderClient, transaction);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    private void processTransactionResponse(LeaderClient leaderClient, Transaction transaction) {
        if (transaction.getAction() == TransactionType.Action.ACKNOWLEDGE) {
            if (transactionManager.hasTransaction(transaction.getTransactionId())) {
                transactionManager.updateTransaction3PhaseStatus(transaction.getTransactionId(),
                        leaderClient.getNode(),
                        ThreePhaseTransactionStatus.ACK);
                if (transactionManager.readyToCommit(transaction.getTransactionId())) {
                    transactionQueueProcessor.push(
                            new CommitRequestTransaction(transaction.getTransactionId())
                            , true
                    );
                }
            }
        } else if (transaction.getAction() == TransactionType.Action.PASS) {
            transactionQueueProcessor.push(new AbortRequestTransaction(transaction.getTransactionId()), true);
        } else if (transaction.getAction() == TransactionType.Action.SUCCESS) {
            final RequestTransaction managerTransaction = (RequestTransaction) transactionManager.getTransaction(transaction.getTransactionId());
            if (managerTransaction.getRequestType() == TransactionType.RequestType.CONSUMER_RECORD_READ) {
                router.routeToClient(
                        new ConsumerRecordResponse(
                                nodeTransactionAndClientRequestMapper.get(transaction.getTransactionId()),
                                convertTransactionTypeToRequestType(managerTransaction.getRequestType()),
                                KafkaClientResponse.ResponseType.SUCCESS,
                                ((ConsumerRecordReadResponseTransaction) transaction).getConsumerRecords()
                        )
                );

                transactionManager.deleteTransaction(transaction.getTransactionId());
                nodeTransactionAndClientRequestMapper.delete(transaction.getTransactionId());
            }
        }
    }

    public void processNodeConfiguration(LeaderClient leaderClient, LFRequest lfRequest) {
        System.out.println(lfRequest);
        if (lfRequest.getLfRequestType() == LFRequest.LFRequestType.NEW) {
            try {
                leaderClient.setLeaderClientState(LeaderClientState.CONNECTED);
                leaderClient.send(serializer.serialize(new LFResponse(LFResponse.LFResponseType.CONNECTED)));
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else if (lfRequest.getLfRequestType() == LFRequest.LFRequestType.CONFIGURE) {
            try {
                leaderClient.setLeaderClientState(LeaderClientState.CONFIGURED);
                leaderClient.send(serializer.serialize(new LFResponse(LFResponse.LFResponseType.CONFIGURED)));
                leaderClient.setNode(new Node(lfRequest.getNodeId(), leaderClient.getNode().getIpAddress()));
                registerNewNode(leaderClient);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void registerNewNode(LeaderClient leaderClient) {
        nodeMapper.add(leaderClient.getNode().getId(), leaderClient);
        nodeManager.registerNode(leaderClient.getNode());
        // add node to cluster
    }

    public void unRegisterNode(LeaderClient leaderClient) {
        nodeMapper.delete(leaderClient.getNode().getId());
        nodeManager.unRegisterNode(leaderClient.getNode());
        // remove from cluster, mark as inactive
    }

    public void sendToNode(LeaderClient leaderClient, Transaction transaction) {
        try {
            leaderClient.send(serializer.serialize(transaction));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private KafkaClientRequest.RequestType convertTransactionTypeToRequestType(TransactionType.RequestType requestType) {
        return switch (requestType) {
            case TOPIC_CREATE, TOPIC_PARTITION_CREATE -> KafkaClientRequest.RequestType.TOPIC_CREATE;
            case TOPIC_DELETE, TOPIC_PARTITION_DELETE -> KafkaClientRequest.RequestType.TOPIC_DELETE;
            case PRODUCER_RECORD_WRITE -> KafkaClientRequest.RequestType.PRODUCER_RECORD_WRITE;
            case CONSUMER_RECORD_READ -> KafkaClientRequest.RequestType.CONSUMER_RECORD_READ;
            default -> null;
        };
    }
}
