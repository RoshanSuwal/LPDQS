package org.ekbana.server.Application;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.ekbana.server.common.cm.request.AuthRequest;
import org.ekbana.server.common.cm.request.ConsumerConfigRequest;
import org.ekbana.server.common.cm.request.ConsumerRecordReadRequest;

import java.io.IOException;

public class KafkaConsumerClientApplication {
    public static long offset=0;

    static KafkaClientApplication.KafkaHandler kafkaHandler=new KafkaClientApplication.KafkaHandler() {
        @Override
        public void read(String msg) {
            System.out.println(msg.trim());
            try {
                final JsonElement jsonElement = JsonParser.parseString(msg.trim());
                final JsonObject asJsonObject = jsonElement.getAsJsonObject();
//                System.out.println(asJsonObject);
                final long endingOffset = asJsonObject.get("consumerRecords").getAsJsonObject().get("endingOffset").getAsLong();
                offset = Math.max(offset,endingOffset + 1);
            }catch (Exception e){
                System.out.println(e.getMessage());
            }
        }
    };

    static KafkaClientApplication kafkaClient=new KafkaClientApplication("localhost",9999,kafkaHandler);

    static void send(long offset) throws IOException, InterruptedException {
        kafkaClient.write(new Gson().toJson(new ConsumerRecordReadRequest("tweet-19500", 0, offset, false)));
    }

    public static void main(String[] args) throws IOException, InterruptedException {

//        KafkaClientApplication kafkaClient=new KafkaClientApplication("localhost",9999,kafkaHandler);
        kafkaClient.startClient();

        kafkaClient.write(new Gson().toJson(new AuthRequest()));

        final ConsumerConfigRequest consumerConfigRequest = new ConsumerConfigRequest("tweet-19500", "group-0", new int[]{0});
        kafkaClient.write(new Gson().toJson(consumerConfigRequest));

//        Thread.sleep(200);
//        final ConsumerRecordReadRequest consumerRecordReadRequest = new ConsumerRecordReadRequest("tweet-19500", 0, 0, false);
//        kafkaClient.write(new Gson().toJson(consumerRecordReadRequest));
//        kafkaClient.write(new Gson().toJson(new ConsumerRecordReadRequest("tweet-19500", 0, -1000, false)));

        while (true) {
            Thread.sleep(1000);
            send(offset);
        }
    }
}
