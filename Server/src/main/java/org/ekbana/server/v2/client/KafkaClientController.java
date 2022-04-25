package org.ekbana.server.v2.client;

import org.ekbana.minikafka.common.Mapper;
import org.ekbana.server.common.cm.request.InvalidRequest;
import org.ekbana.server.common.cm.request.KafkaClientRequest;
import org.ekbana.server.common.cm.response.KafkaClientResponse;
import org.ekbana.server.util.QueueProcessor;
import org.ekbana.server.v2.common.KafkaRouter;
import org.ekbana.server.v2.common.Router;

import java.util.concurrent.ExecutorService;

public class KafkaClientController {
    // controls the client
    // Mapper to store client request id and clientId
    private Mapper<Long,KafkaClient> clientRequestMapper;

    private KafkaClientRequestParser kafkaClientRequestParser;

    private Router.KafkaClientRouter kafkaRouter;

    private QueueProcessor<KafkaClientResponse> kafkaClientResponseQueueProcessor;

    public KafkaClientController( KafkaClientRequestParser kafkaClientRequestParser, Router.KafkaClientRouter kafkaRouter, ExecutorService executorService) {
        this.kafkaClientRequestParser = kafkaClientRequestParser;
        this.kafkaRouter = kafkaRouter;
        this.clientRequestMapper = new Mapper<>();
        this.kafkaClientResponseQueueProcessor = new QueueProcessor<>(100, kafkaClientResponse -> sendToClient(kafkaClientResponse),executorService);
    }

    public void request(KafkaClient kafkaClient, byte[] bytes){
        request(kafkaClient,kafkaClientRequestParser.parse(bytes));
    }

    public void request(KafkaClient kafkaClient,KafkaClientRequest kafkaClientRequest){
        final KafkaClientRequest processedRequest = ClientProcessor.processRequest(kafkaClient, kafkaClientRequest);
        System.out.println(processedRequest);
        clientRequestMapper.add(processedRequest.getClientRequestId(), kafkaClient);
        // route request to leader for further procession
        kafkaRouter.routeToLeader(processedRequest);
    }

    public void response(KafkaClientResponse kafkaClientResponse){
        // send response back to client
        kafkaClientResponseQueueProcessor.push(kafkaClientResponse,false);
    }

    public void sendToClient(KafkaClientResponse kafkaClientResponse){
        System.out.println(kafkaClientResponse);
        if (clientRequestMapper.has(kafkaClientResponse.getClientResponseId())){
            clientRequestMapper.get(kafkaClientResponse.getClientResponseId())
                    .send(ClientProcessor.processResponse(clientRequestMapper.get(kafkaClientResponse.getClientResponseId()),kafkaClientResponse));
        }

        clientRequestMapper.delete(kafkaClientResponse.getClientResponseId());
    }

    private static class IdGenerator{
        private static Long id=0L;// randomise on start

        public static Long generate(){
            id=id+1;
            return id;
        }
    }

    private static class ClientProcessor{

        public static KafkaClientRequest processRequest(KafkaClient kafkaClient,KafkaClientRequest kafkaClientRequest){
            System.out.println("[Client] ["+ kafkaClient.getKafkaClientState()+"] "+kafkaClientRequest.getRequestType());
            kafkaClientRequest.setClientRequestId(IdGenerator.generate());

            if (kafkaClientRequest.getRequestType()== KafkaClientRequest.RequestType.NON_PARSABLE){
                return kafkaClientRequest;
            }else if (kafkaClientRequest.getRequestType()== KafkaClientRequest.RequestType.CLOSE_CLIENT){
                return kafkaClientRequest;
            }

            switch (kafkaClient.getKafkaClientState()){
                case NEW -> {
                    kafkaClient.setKafkaClientState(KafkaClientState.CONNECTING);
                    if (kafkaClientRequest.getRequestType()!= KafkaClientRequest.RequestType.NEW_CONNECTION){
                        return new InvalidRequest(kafkaClientRequest.getClientRequestId(),"Expecting new connection request");
                    }
                }case CONNECTED ->{
                    if (kafkaClientRequest.getRequestType()== KafkaClientRequest.RequestType.TOPIC_CREATE){
                        kafkaClient.setKafkaClientState(KafkaClientState.CREATING_TOPIC);
                    }else if (kafkaClientRequest.getRequestType()== KafkaClientRequest.RequestType.TOPIC_DELETE){
                        kafkaClient.setKafkaClientState(KafkaClientState.DELETING_TOPIC);
                    }else if (kafkaClientRequest.getRequestType()== KafkaClientRequest.RequestType.PRODUCER_CONFIG){
                        kafkaClient.setKafkaClientState(KafkaClientState.CONFIGURED_PRODUCER);
                    }else if (kafkaClientRequest.getRequestType()== KafkaClientRequest.RequestType.CONSUMER_CONFIG){
                        kafkaClient.setKafkaClientState(KafkaClientState.CONFIGURING_CONSUMER);
                    }else {
                        return new InvalidRequest(kafkaClientRequest.getClientRequestId(), "expecting configuration request");
                    }
                }case CONFIGURED_PRODUCER -> {
                    if (kafkaClientRequest.getRequestType()!= KafkaClientRequest.RequestType.PRODUCER_RECORD_WRITE){
                        return new InvalidRequest(kafkaClientRequest.getClientRequestId(), "expecting record write request");
                    }
                }case CONFIGURED_CONSUMER -> {
                    if (kafkaClientRequest.getRequestType()!= KafkaClientRequest.RequestType.CONSUMER_RECORD_READ &&
                            kafkaClientRequest.getRequestType()!=KafkaClientRequest.RequestType.CONSUMER_OFFSET_COMMIT){
                        return new InvalidRequest(kafkaClientRequest.getClientRequestId(), "expecting consumer read request or offset commit request");
                    }
                }case CLOSED -> {
                    return new InvalidRequest(kafkaClientRequest.getClientRequestId(), "Connection already closed");
                }
            }

            return kafkaClientRequest;
        }


        public static KafkaClientResponse processResponse(KafkaClient kafkaClient, KafkaClientResponse kafkaClientResponse){
            // kafka client state management
            if (kafkaClientResponse.getResponseType()== KafkaClientResponse.ResponseType.SUCCESS){
                switch (kafkaClient.getKafkaClientState()){
                    case CONNECTING -> kafkaClient.setKafkaClientState(KafkaClientState.CONNECTED);
                    case CREATING_TOPIC, DELETED_TOPIC -> kafkaClient.setKafkaClientState(KafkaClientState.CLOSE);
                    case CONFIGURING_PRODUCER -> kafkaClient.setKafkaClientState(KafkaClientState.CONFIGURED_PRODUCER);
                    case CONFIGURING_CONSUMER -> kafkaClient.setKafkaClientState(KafkaClientState.CONFIGURED_CONSUMER);
                }
            }else {
                if (kafkaClient.getKafkaClientState()!= KafkaClientState.CONFIGURED_CONSUMER ||
                        kafkaClient.getKafkaClientState()!= KafkaClientState.CONFIGURED_PRODUCER){
                    kafkaClient.setKafkaClientState(KafkaClientState.CLOSE);
                }
            }

            return kafkaClientResponse;
        }
    }
}
