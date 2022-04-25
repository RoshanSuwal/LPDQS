package org.ekbana.server.v2.leader;

import org.ekbana.server.cluster.Node;
import org.ekbana.server.common.cm.request.*;
import org.ekbana.server.common.cm.response.BaseResponse;
import org.ekbana.server.common.cm.response.ConsumerConfigResponse;
import org.ekbana.server.common.cm.response.KafkaClientResponse;
import org.ekbana.server.common.cm.response.ProducerConfigResponse;
import org.ekbana.server.common.mb.Topic;
import org.ekbana.server.common.mb.Transaction;
import org.ekbana.server.util.Mapper;
import org.ekbana.server.v2.common.Router;
import org.ekbana.server.v2.node.NodeManager;

public class LeaderController {

    private Router.LeaderRouter router;
    // routes the request between client and nodes

    private TopicController topicController;

    private Mapper<Long, Long> nodeTransactionAndClientRequestMapper;
    private NodeManager nodeManager;

    public LeaderController(Router.LeaderRouter router, TopicController topicController,NodeManager nodeManager) {
        this.router = router;
        this.topicController = topicController;
        nodeTransactionAndClientRequestMapper=new Mapper<>();
        this.nodeManager=nodeManager;
    }

    public void request(KafkaClientRequest kafkaClientRequest) {
        switch (kafkaClientRequest.getRequestType()) {
            // configure the producer
            case PRODUCER_CONFIG -> {
                // check if topic exists and return the response according to it
                if (!topicController.hasTopic(((ProducerConfigRequest) kafkaClientRequest).getTopicName())) {
                    router.routeToClient(
                            new BaseResponse(
                                    kafkaClientRequest.getClientRequestId(),
                                    kafkaClientRequest.getRequestType(),
                                    KafkaClientResponse.ResponseType.FAIL,
                                    "topic does not exists"
                            )
                    );
                } else {
                    router.routeToClient(
                            new ProducerConfigResponse(
                                    kafkaClientRequest.getClientRequestId(),
                                    topicController.getTopic(((ProducerConfigRequest) kafkaClientRequest).getTopicName())
                            )
                    );
                }
            }
            case PRODUCER_RECORD_WRITE -> {
                // use producer controller
                ProducerRecordWriteRequest producerRecordWriteRequest = (ProducerRecordWriteRequest) kafkaClientRequest;

                if (topicController.hasTopic(producerRecordWriteRequest.getTopicName())) {
                    final Topic topic = topicController.getTopic(producerRecordWriteRequest.getTopicName());

                    router.routeToNode(kafkaClientRequest,topic);
//                    final Long generateTransactionId = Generator.generateTransactionId();
//                    router.routeToNode(
//                            new ProducerRecordWriteRequestTransaction(
//                                    generateTransactionId,
//                                    TransactionType.Action.REGISTER,
//                                    topic,
//                                    producerRecordWriteRequest.getPartitionId(),
//                                    producerRecordWriteRequest.getProducerRecords()
//                            )
//                    );
//                    nodeTransactionAndClientRequestMapper.add(generateTransactionId, kafkaClientRequest.getClientRequestId());

                } else {
                    router.routeToClient(
                        new BaseResponse(
                                kafkaClientRequest.getClientRequestId(),
                                kafkaClientRequest.getRequestType(),
                                KafkaClientResponse.ResponseType.FAIL,
                                "topic does not exists"
                        )
                    );
                }
            }case CONSUMER_CONFIG -> {
                // configure the consumer
                if (!topicController.hasTopic(((ConsumerConfigRequest) kafkaClientRequest).getTopicName())) {
                    router.routeToClient(
                            new BaseResponse(
                                    kafkaClientRequest.getClientRequestId(),
                                    kafkaClientRequest.getRequestType(),
                                    KafkaClientResponse.ResponseType.FAIL,
                                    "topic does not exists"
                            )
                    );
                } else {
                    router.routeToClient(
                            new ConsumerConfigResponse(
                                    kafkaClientRequest.getClientRequestId(),
                                    topicController.getTopic(((ConsumerConfigRequest) kafkaClientRequest).getTopicName())
                            )
                    );
                }

                // check if topic exists
                // return response according to it
            }
            case CONSUMER_RECORD_READ -> {
                // use producer controller
                ConsumerRecordReadRequest consumerRecordReadRequest = (ConsumerRecordReadRequest) kafkaClientRequest;

                if (topicController.hasTopic(consumerRecordReadRequest.getTopicName())) {
                    final Topic topic = topicController.getTopic(consumerRecordReadRequest.getTopicName());
                    router.routeToNode(kafkaClientRequest,topic);
                } else {
                    router.routeToClient(
                            new BaseResponse(
                                    kafkaClientRequest.getClientRequestId(),
                                    kafkaClientRequest.getRequestType(),
                                    KafkaClientResponse.ResponseType.FAIL,
                                    "topic does not exists"
                            )
                    );
                }
            }
            case CONSUMER_OFFSET_COMMIT -> {
                // commit the offset in leader
                // created consumer controller that deals with consumer only
            }
            case TOPIC_CREATE -> {
                // check if topic exists
                if (topicController.hasTopic(((TopicCreateRequest) kafkaClientRequest).getTopicName())) {
                    // if exists return topic already exists response
                    router.routeToClient(
                            new BaseResponse(
                                    kafkaClientRequest.getClientRequestId(),
                                    kafkaClientRequest.getRequestType(),
                                    KafkaClientResponse.ResponseType.FAIL,
                                    "topic already exists"
                            )
                    );
                } else {
                    //create new topic
                    final TopicCreateRequest topicCreateRequest = (TopicCreateRequest) kafkaClientRequest;
                    final Topic newTopic = Topic.builder()
                            .topicName(topicCreateRequest.getTopicName())
                            .numberOfPartitions(topicCreateRequest.getNumberOfPartitions())
                            .dataNode(nodeManager.getNodes(topicCreateRequest.getNumberOfPartitions()))
                            .build();
                    System.out.println(newTopic);
                    // get the nodes for topic
                    //
                    // else perform partitioning and send to each partition nodes
                    router.routeToNode(kafkaClientRequest,newTopic);
//                    final Long transactionId = Generator.generateTransactionId();
//                    router.routeToNode(
//                            new TopicCreateRequestTransaction(
//                                    transactionId,
//                                    TransactionType.Action.REGISTER,
//                                    newTopic,
//                                    TransactionType.RequestType.TOPIC_PARTITION_CREATE
//                            )
//                    );
//                    nodeTransactionAndClientRequestMapper.add(transactionId, kafkaClientRequest.getClientRequestId());
                }
            }case TOPIC_DELETE -> {
                // check if topic exists
                if (topicController.hasTopic(((TopicDeleteRequest) kafkaClientRequest).getTopicName())) {
                    final Topic deleteTopic = topicController.getTopic(((TopicDeleteRequest) kafkaClientRequest).getTopicName());
//                    final Long generateTransactionId = Generator.generateTransactionId();
//                    router.routeToNode(
//                            new TopicDeleteRequestTransaction(
//                                    generateTransactionId,
//                                    TransactionType.Action.REGISTER,
//                                    deleteTopic,
//                                    TransactionType.RequestType.TOPIC_PARTITION_DELETE
//                            )
//                    );
//                    nodeTransactionAndClientRequestMapper.add(generateTransactionId, kafkaClientRequest.getClientRequestId());
                    router.routeToNode(kafkaClientRequest,deleteTopic);
                } else {
                    router.routeToClient(
                            new BaseResponse(
                                    kafkaClientRequest.getClientRequestId(),
                                    kafkaClientRequest.getRequestType(),
                                    KafkaClientResponse.ResponseType.FAIL,
                                    "topic does not exists"
                            )
                    );
                }
                // if exists send delete request to all partition and replica nodes
                // else return topic not exists response
            }case NEW_CONNECTION -> {
                router.routeToClient(
                        new BaseResponse(
                                kafkaClientRequest.getClientRequestId(),
                                kafkaClientRequest.getRequestType(),
                                KafkaClientResponse.ResponseType.SUCCESS,
                                "connected successfully"
                        )
                );
            }case NON_PARSABLE -> {
                router.routeToClient(
                        new BaseResponse(
                                kafkaClientRequest.getClientRequestId(),
                                kafkaClientRequest.getRequestType(),
                                KafkaClientResponse.ResponseType.FAIL,
                                "non parsable request"
                        )
                );
            }default -> {

            }
        }
        // register request in queue
        // process the request
        // create transaction
        // perform load balancing
        // get the nodes
        // send to nodes
    }

    public void response(KafkaClientResponse kafkaClientResponse) {
        // send response to client
        router.routeToClient(kafkaClientResponse);
    }

    public void transaction(Transaction transaction) {

    }
}
