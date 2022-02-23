package org.ekbana.server.Application;

import com.google.gson.Gson;
import org.ekbana.server.common.cm.request.AuthRequest;
import org.ekbana.server.common.cm.request.ProducerConfigRequest;
import org.ekbana.server.common.cm.request.ProducerRecordWriteRequest;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class KafkaProducerClientApplication {
    public static void main(String[] args) throws IOException, InterruptedException {
        KafkaClientApplication.KafkaHandler kafkaProducerHandler= System.out::println;

        KafkaClientApplication kafkaClientApplication = new KafkaClientApplication("localhost",9999,kafkaProducerHandler);

        kafkaClientApplication.startClient();

        kafkaClientApplication.write(new Gson().toJson(new AuthRequest()));

        kafkaClientApplication.write(new Gson().toJson(new ProducerConfigRequest("tweet-19500")));

        List<String> records= Arrays.asList("hello","world","first record","second record");
        final ProducerRecordWriteRequest producerRecordWriteRequest = new ProducerRecordWriteRequest();
        producerRecordWriteRequest.setTopicName("tweet-19500");
        producerRecordWriteRequest.setProducerRecords(records);

//        kafkaClient.write(new Gson().toJson(producerRecordWriteRequest));

        while (true){
            kafkaClientApplication.write(new Gson().toJson(producerRecordWriteRequest));
            Thread.sleep(1000);
        }
    }
}
