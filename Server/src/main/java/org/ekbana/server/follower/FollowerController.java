package org.ekbana.server.follower;

import org.ekbana.server.common.Router;
import org.ekbana.server.common.cm.request.KafkaClientRequest;
import org.ekbana.server.common.cm.response.BaseResponse;
import org.ekbana.server.common.cm.response.KafkaClientResponse;
import org.ekbana.server.common.l.*;
import org.ekbana.server.common.lr.RTransaction;
import org.ekbana.server.common.mb.Transaction;
import org.ekbana.server.config.KafkaProperties;
import org.ekbana.server.util.Deserializer;
import org.ekbana.server.util.QueueProcessor;
import org.ekbana.server.util.Serializer;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

public class FollowerController {

    private final KafkaProperties kafkaProperties;
    private final Serializer serializer;
    private final Deserializer deserializer;
    private final ExecutorService executorService;

    private final Router.KafkaFollowerRouter kafkaFollowerRouter;

    private final QueueProcessor<Object> requestQueueProcessor;
    private final Follower follower;

    public FollowerController(KafkaProperties kafkaProperties,Follower follower, Serializer serializer, Deserializer deserializer, Router.KafkaFollowerRouter kafkaFollowerRouter, ExecutorService executorService) {
        this.serializer = serializer;
        this.deserializer = deserializer;
        this.kafkaFollowerRouter = kafkaFollowerRouter;
        this.kafkaProperties = kafkaProperties;
        this.executorService = executorService;
        this.follower=follower;

        Listener<byte[]> followerClientReadListener = this::rawData;
        follower.registerListener(followerClientReadListener);

        QueueProcessor.QueueProcessorListener<Object> objectQueueProcessorListener = o -> {
            try {
                if (follower.getFollowerState()== Follower.FollowerState.AUTHENTICATED || o instanceof LFRequest) {
                    log("leader",o);
                    follower.write(serializer.serialize(o));
                    Thread.sleep(100);
                }
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        };

        this.requestQueueProcessor = new QueueProcessor<>(100, objectQueueProcessorListener,executorService);
    }

    private void log(String fromTo,Object obj){
        System.out.println("[Follower] ["+fromTo+"] "+obj);
    }

    public void registerRequest(KafkaClientRequest kafkaClientRequest){
        log("client",kafkaClientRequest);
        // some rules
        if (kafkaClientRequest.getRequestType()== KafkaClientRequest.RequestType.NEW_CONNECTION){
            processKafkaClientResponse(new BaseResponse(kafkaClientRequest.getClientRequestId(),kafkaClientRequest.getRequestType(), KafkaClientResponse.ResponseType.SUCCESS,"Connected successfully"));
        }else if (kafkaClientRequest.getRequestType()== KafkaClientRequest.RequestType.AUTH){
            // authentication logic
            processKafkaClientResponse(new BaseResponse(kafkaClientRequest.getClientRequestId(),kafkaClientRequest.getRequestType(), KafkaClientResponse.ResponseType.SUCCESS,"Authenticated successfully"));
        }else if (kafkaClientRequest.getRequestType()== KafkaClientRequest.RequestType.INVALID){
            processKafkaClientResponse(new BaseResponse(kafkaClientRequest.getClientRequestId(),kafkaClientRequest.getRequestType(), KafkaClientResponse.ResponseType.FAIL,"invalid request"));
        }else if (kafkaClientRequest.getRequestType()== KafkaClientRequest.RequestType.NON_PARSABLE){
            processKafkaClientResponse(new BaseResponse(kafkaClientRequest.getClientRequestId(),kafkaClientRequest.getRequestType(), KafkaClientResponse.ResponseType.FAIL,"Non Parsable request"));
        }else {
//            requestQueueProcessor.push(kafkaClientRequest, false);
            requestQueueProcessor.push(new LRequest(FollowerMode.MODE_CLIENT,kafkaClientRequest),false);
        }
    }

    public void registerTransaction(Transaction transaction){
//        requestQueueProcessor.push(transaction,false);
        log("broker",transaction);
        requestQueueProcessor.push(new LResponse(FollowerMode.MODE_DATA,transaction),false);
    }

    public void registerRTransaction(RTransaction rTransaction){
        log("replica",rTransaction);
        requestQueueProcessor.push(new LResponse(FollowerMode.MODE_REPLICA,rTransaction),false);
    }

    public void rawData(byte[] bytes){
//        System.out.println("[raw data] "+new String(bytes));
        try {
            final Object deserialized = deserializer.deserialize(bytes);
            log("leader",deserialized);
            if (deserialized instanceof LFResponse){
                processLFResponse((LFResponse) deserialized);
            }else if (deserialized instanceof KafkaClientResponse){
                processKafkaClientResponse((KafkaClientResponse) deserialized);
            }else if (deserialized instanceof Transaction){
                processTransaction((Transaction) deserialized);
            }else if (deserialized instanceof RTransaction){
                processRTransaction((RTransaction) deserialized);
            }
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    private void processLFResponse(LFResponse lfResponse){
        if (lfResponse.getLfResponseType()== LFResponse.LFResponseType.CONNECTED){
            follower.setFollowerState(Follower.FollowerState.CONNECTED);
            requestQueueProcessor.push(new LFRequest(kafkaProperties.getKafkaProperty("kafka.server.node.id"), kafkaProperties.getKafkaProperty("kafka.security.auth.username"), kafkaProperties.getKafkaProperty("kafka.security.auth.password"), LFRequest.LFRequestType.AUTH),true);
        }else if (lfResponse.getLfResponseType()== LFResponse.LFResponseType.AUTHENTICATED){
            follower.setFollowerState(Follower.FollowerState.AUTHENTICATED);
        }else if (lfResponse.getLfResponseType()== LFResponse.LFResponseType.UNAUTHENTICATED){
            follower.setFollowerState(Follower.FollowerState.CLOSE);
            follower.close();
        }
    }

    public void processKafkaClientResponse(KafkaClientResponse kafkaClientResponse){
        kafkaFollowerRouter.routeFromFollowerToClient(kafkaClientResponse);
    }

    public void processTransaction(Transaction transaction){
        // deals with broker
        kafkaFollowerRouter.routeFromFollowerToBroker(transaction);
    }

    public void processRTransaction(RTransaction rTransaction){
        kafkaFollowerRouter.routeFromFollowerToReplica(rTransaction);
    }

    public interface Listener<T>{
        void onListen(T t);
    }
}
