package org.ekbana.server.client;

import org.ekbana.server.common.Router;
import org.ekbana.server.common.cm.request.KafkaClientRequest;
import org.ekbana.server.common.cm.response.KafkaClientResponse;
import org.ekbana.server.util.Mapper;
import org.ekbana.server.util.QueueProcessor;

import java.util.concurrent.ExecutorService;

public class KafkaClientController {
    private final KafkaClientConfig kafkaClientConfig;
    private final KafkaClientRequestParser kafkaClientRequestParser;
    private final KafkaClientProcessor kafkaClientProcessor;
    private final Router.KafkaClientRouter kafkaClientRouter;

    private final Mapper<Long,KafkaClient> kafkaClientRequestMapper;
    private Long clientRequestId=0L;

    private final QueueProcessor<KafkaClientResponse> kafkaClientResponseQueueProcessor;

    public KafkaClientController(KafkaClientConfig kafkaClientConfig, KafkaClientRequestParser kafkaClientRequestParser, KafkaClientProcessor kafkaClientProcessor, Router.KafkaClientRouter kafkaClientRouter, Mapper<Long, KafkaClient> kafkaClientRequestMapper, ExecutorService executorService) {
        this.kafkaClientConfig = kafkaClientConfig;
        this.kafkaClientRequestParser = kafkaClientRequestParser;
        this.kafkaClientProcessor = kafkaClientProcessor;
        this.kafkaClientRouter = kafkaClientRouter;
        this.kafkaClientRequestMapper = kafkaClientRequestMapper;
        QueueProcessor.QueueProcessorListener<KafkaClientResponse> kafkaClientResponseQueueProcessorListener = this::send;
        kafkaClientResponseQueueProcessor=new QueueProcessor<>(100, kafkaClientResponseQueueProcessorListener,executorService);
    }

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
        log("api",kafkaClientRequest);
        // validation and processing of the request
        KafkaClientRequest kafkaClientProcessedRequest = kafkaClientProcessor.processRequest(kafkaClient, kafkaClientRequest);
        // send it to server
        log("process",kafkaClientProcessedRequest);
        kafkaClientRouter.routeFromClientToFollower(kafkaClientProcessedRequest);
    }

    private void log(String fromTo,Object object){
        System.out.println("[Client] ["+fromTo+"] "+object);
    }

    public synchronized void  response(KafkaClientResponse kafkaClientResponse){
        kafkaClientResponseQueueProcessor.push(kafkaClientResponse,false);
    }

    public void send(KafkaClientResponse kafkaClientResponse){
        // fetch the kafkaClient using the request response id
        //
        log("api",kafkaClientResponse);
        if (kafkaClientRequestMapper.has(kafkaClientResponse.getClientResponseId())){
            final KafkaClient kafkaClient = kafkaClientRequestMapper.get(kafkaClientResponse.getClientResponseId());
            kafkaClient.send(kafkaClientProcessor.processResponse(kafkaClient,kafkaClientResponse));

        }
        kafkaClientRequestMapper.delete(kafkaClientResponse.getClientResponseId());
    }

}
