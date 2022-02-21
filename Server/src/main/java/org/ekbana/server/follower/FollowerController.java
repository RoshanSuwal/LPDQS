package org.ekbana.server.follower;

import org.ekbana.server.common.Router;
import org.ekbana.server.common.cm.request.KafkaClientRequest;
import org.ekbana.server.common.cm.response.BaseResponse;
import org.ekbana.server.common.cm.response.KafkaClientResponse;
import org.ekbana.server.common.l.FollowerMode;
import org.ekbana.server.common.l.LRequest;
import org.ekbana.server.common.l.LResponse;
import org.ekbana.server.common.mb.Transaction;
import org.ekbana.server.leader.KafkaServerConfig;
import org.ekbana.server.util.Deserializer;
import org.ekbana.server.util.QueueProcessor;
import org.ekbana.server.util.Serializer;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

public class FollowerController {

    private final KafkaServerConfig kafkaServerConfig;
    private final Serializer serializer;
    private final Deserializer deserializer;
    private final ExecutorService executorService;

    private final Router.KafkaFollowerRouter kafkaFollowerRouter;

    private final QueueProcessor<Object> requestQueueProcessor;

    public FollowerController(KafkaServerConfig kafkaServerConfig,Follower follower, Serializer serializer, Deserializer deserializer, Router.KafkaFollowerRouter kafkaFollowerRouter, ExecutorService executorService) {
        this.serializer = serializer;
        this.deserializer = deserializer;
        this.kafkaFollowerRouter = kafkaFollowerRouter;
        this.kafkaServerConfig = kafkaServerConfig;
        this.executorService = executorService;

        Listener<byte[]> followerClientReadListener = this::rawData;
        follower.registerListener(followerClientReadListener);

        QueueProcessor.QueueProcessorListener<Object> objectQueueProcessorListener = o -> {
            try {
                follower.write(serializer.serialize(o));
            } catch (IOException e) {
                e.printStackTrace();
            }
        };

        this.requestQueueProcessor = new QueueProcessor<>(100, objectQueueProcessorListener,executorService);
    }

    public void registerRequest(KafkaClientRequest kafkaClientRequest){
        System.out.println("[FOLLOWER CONTROLLER][KAFKA CLIENT REQUEST] "+kafkaClientRequest);
        // some rules
        if (kafkaClientRequest.getRequestType()== KafkaClientRequest.RequestType.AUTH){
            processKafkaClientResponse(new BaseResponse(kafkaClientRequest.getClientRequestId(),kafkaClientRequest.getRequestType(), KafkaClientResponse.ResponseType.SUCCESS,"Authenticated successfully"));
        }else if (kafkaClientRequest.getRequestType()== KafkaClientRequest.RequestType.NEW_CONNECTION){
            processKafkaClientResponse(new BaseResponse(kafkaClientRequest.getClientRequestId(),kafkaClientRequest.getRequestType(), KafkaClientResponse.ResponseType.SUCCESS,"Connected successfully"));
        }else if (kafkaClientRequest.getRequestType()== KafkaClientRequest.RequestType.INVALID){
            processKafkaClientResponse(new BaseResponse(kafkaClientRequest.getClientRequestId(),kafkaClientRequest.getRequestType(), KafkaClientResponse.ResponseType.FAIL,"invalid request"));
        }else if (kafkaClientRequest.getRequestType()== KafkaClientRequest.RequestType.NON_PARSABLE){
            processKafkaClientResponse(new BaseResponse(kafkaClientRequest.getClientRequestId(),kafkaClientRequest.getRequestType(), KafkaClientResponse.ResponseType.FAIL,"Non Parsable request"));
        }else {
            requestQueueProcessor.push(kafkaClientRequest, false);
            requestQueueProcessor.push(new LRequest(FollowerMode.MODE_CLIENT,kafkaClientRequest),false);
        }
    }

    public void registerTransaction(Transaction transaction){
//        requestQueueProcessor.push(transaction,false);
        System.out.println("[FOLLOWER CONTROLLER][BROKER RESPONSE] "+transaction);
        requestQueueProcessor.push(new LResponse(FollowerMode.MODE_DATA,transaction),false);
    }

    public void rawData(byte[] bytes){
        System.out.println("[raw data] "+new String(bytes));
        try {
            final Object deserialized = deserializer.deserialize(bytes);
            if (deserialized instanceof KafkaClientResponse){
                processKafkaClientResponse((KafkaClientResponse) deserialized);
            }else if (deserialized instanceof Transaction){
                processTransaction((Transaction) deserialized);
            }
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public void processKafkaClientResponse(KafkaClientResponse kafkaClientResponse){
        System.out.println("[FOLLOWER CONTROLLER][KAFKA CLIENT RESPONSE] "+kafkaClientResponse);
        kafkaFollowerRouter.routeFromFollowerToClient(kafkaClientResponse);
    }

    public void processTransaction(Transaction transaction){
        // deals with broker
        System.out.println("[FOLLOWER CONTROLLER][BROKER REQUEST] "+transaction);
        kafkaFollowerRouter.routeFromFollowerToBroker(transaction);
    }

    public interface Listener<T>{
        void onListen(T t);
    }
}
