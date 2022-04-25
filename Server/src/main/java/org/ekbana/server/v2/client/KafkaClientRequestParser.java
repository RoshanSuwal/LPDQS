package org.ekbana.server.v2.client;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.ekbana.server.common.cm.request.*;

public class KafkaClientRequestParser {

    public KafkaClientRequest parse(byte[] bytes){
        final String json = new String(bytes);
        System.out.println(json);
        final JsonObject jsonObject = new Gson().fromJson(json, JsonObject.class);
        return jsonObject.has("requestType")?
               switch (KafkaClientRequest.RequestType.valueOf(jsonObject.get("requestType").getAsString())){
                   case AUTH -> new Gson().fromJson(jsonObject, AuthRequest.class);
                   case TOPIC_CREATE -> new Gson().fromJson(jsonObject, TopicCreateRequest.class);
                   case TOPIC_DELETE -> new Gson().fromJson(jsonObject, TopicDeleteRequest.class);
                   case PRODUCER_CONFIG -> new Gson().fromJson(jsonObject,ProducerConfigRequest.class);
                   case PRODUCER_RECORD_WRITE -> new Gson().fromJson(jsonObject,ProducerRecordWriteRequest.class);
                   case CONSUMER_CONFIG -> new Gson().fromJson(jsonObject,ConsumerConfigRequest.class);
                   case CONSUMER_RECORD_READ -> new Gson().fromJson(jsonObject,ConsumerRecordReadRequest.class);
                   case CONSUMER_OFFSET_COMMIT -> new Gson().fromJson(jsonObject,ConsumerCommitOffsetRequest.class);
                   case CLOSE_CLIENT ->new Gson().fromJson(jsonObject,CloseClientRequest.class);
                   case INVALID -> new InvalidRequest("invalid request");
                   case NON_PARSABLE -> new NonParsableRequest();
                   case NEW_CONNECTION -> new NewConnectionRequest();
               }:new NonParsableRequest();
    }

    public static void main(String[] args) {
        KafkaClientRequestParser kafkaClientRequestParser=new KafkaClientRequestParser();
        kafkaClientRequestParser.parse(new Gson().toJson(new AuthRequest()).getBytes());
        kafkaClientRequestParser.parse(new Gson().toJson(new TopicCreateRequest("test",1)).getBytes());
    }
}
