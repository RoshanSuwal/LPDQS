package org.ekbana.server.client;

import org.ekbana.server.common.cm.request.InvalidRequest;
import org.ekbana.server.common.cm.request.KafkaClientRequest;
import org.ekbana.server.common.cm.response.KafkaClientResponse;

public class KafkaClientProcessor {

    // returns processed request
    public KafkaClientRequest processRequest(KafkaClient kafkaClient,KafkaClientRequest kafkaClientRequest){
        // kafka client state management

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
            }case CONNECTED -> {
                kafkaClient.setKafkaClientState(KafkaClientState.AUTHENTICATING);
                if (kafkaClientRequest.getRequestType()!= KafkaClientRequest.RequestType.AUTH){
                    return new InvalidRequest(kafkaClientRequest.getClientRequestId(), "Expecting auth request");
                }
            }case AUTHENTICATED -> {
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
                if (kafkaClientRequest.getRequestType()!= KafkaClientRequest.RequestType.CONSUMER_RECORD_READ ||
                    kafkaClientRequest.getRequestType()!=KafkaClientRequest.RequestType.CONSUMER_OFFSET_COMMIT){
                    return new InvalidRequest(kafkaClientRequest.getClientRequestId(), "expecting consumer read request or offset commit request");
                }
            }case CLOSED -> {
                return new InvalidRequest(kafkaClientRequest.getClientRequestId(), "Connection already closed");
            }
        }

        return kafkaClientRequest;
    }

    // returns kafkaClientResponse
    public KafkaClientResponse processResponse(KafkaClient kafkaClient, KafkaClientResponse kafkaClientResponse){
        // kafka client state management
        if (kafkaClientResponse.getResponseType()== KafkaClientResponse.ResponseType.SUCCESS){
            switch (kafkaClient.getKafkaClientState()){
                case CONNECTING -> kafkaClient.setKafkaClientState(KafkaClientState.CONNECTED);
                case AUTHENTICATING -> kafkaClient.setKafkaClientState(KafkaClientState.AUTHENTICATED);
                case CREATING_TOPIC, DELETED_TOPIC -> kafkaClient.setKafkaClientState(KafkaClientState.CLOSE);
                case CONFIGURING_PRODUCER -> kafkaClient.setKafkaClientState(KafkaClientState.CONFIGURED_PRODUCER);
                case CONFIGURING_CONSUMER -> kafkaClient.setKafkaClientState(KafkaClientState.CONFIGURED_CONSUMER);
            }
        }else {
            if (kafkaClient.getKafkaClientState()!= KafkaClientState.CONFIGURED_CONSUMER ||
                kafkaClient.getKafkaClientState()!=KafkaClientState.CONFIGURED_PRODUCER){
                kafkaClient.setKafkaClientState(KafkaClientState.CLOSE);
            }
        }

        return kafkaClientResponse;
    }
}
