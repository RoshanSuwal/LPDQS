package org.ekbana.server.client;

import com.google.gson.Gson;
import lombok.RequiredArgsConstructor;
import org.ekbana.server.common.Router;
import org.ekbana.server.common.cm.request.KafkaClientRequest;
import org.ekbana.server.common.cm.response.KafkaClientResponse;
import org.ekbana.server.util.Mapper;

import java.util.HashMap;
import java.util.Map;

@RequiredArgsConstructor
public class KafkaClientController {
    private final KafkaClientConfig kafkaClientConfig;
    private final KafkaClientRequestParser kafkaClientRequestParser;
    private final KafkaClientProcessor kafkaClientProcessor;
    private final Router.KafkaClientRouter kafkaClientRouter;

    private final Mapper<Long,KafkaClient> kafkaClientRequestMapper;
    private Long clientRequestId=0L;

    public Long getClientRequestId(){
        clientRequestId=clientRequestId+1;
        return clientRequestId;
    }

    public void rawRequest(KafkaClient kafkaClient, byte[] kafkaClientRequestByte){
        // parsing the request
        request(kafkaClient,kafkaClientRequestParser.parse(kafkaClientRequestByte));
   }

    public void request(KafkaClient kafkaClient,KafkaClientRequest kafkaClientRequest){
        kafkaClientRequest.setClientRequestId(getClientRequestId());
        kafkaClientRequestMapper.add(kafkaClientRequest.getClientRequestId(),kafkaClient);

        System.out.println("[KAFKA CLIENT SERVER][REQUEST] "+kafkaClientRequest);
        // validation and processing of the request
        KafkaClientRequest kafkaClientProcessedRequest = kafkaClientProcessor.processRequest(kafkaClient, kafkaClientRequest);
        // send it to server
        System.out.println("[KAFKA CLIENT SERVER][PROCESS REQUEST]  "+kafkaClientRequest);
        kafkaClientRouter.routeFromClientToFollower(kafkaClientProcessedRequest);
    }

    public synchronized void  response(KafkaClientResponse kafkaClientResponse){

        System.out.println("[KAFKA CLIENT SERVER][RESPONSE] "+kafkaClientResponse);
        // fetch the kafkaClient using the request response id
        //
        if (kafkaClientRequestMapper.has(kafkaClientResponse.getClientResponseId())){
            kafkaClientRequestMapper.get(kafkaClientResponse.getClientResponseId()).send(kafkaClientResponse);
        }
        kafkaClientRequestMapper.delete(kafkaClientResponse.getClientResponseId());
    }




}
