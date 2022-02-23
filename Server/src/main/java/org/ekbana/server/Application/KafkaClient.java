package org.ekbana.server.Application;

import com.google.gson.Gson;
import org.ekbana.server.common.cm.request.AuthRequest;
import org.ekbana.server.common.cm.request.ProducerConfigRequest;
import org.ekbana.server.common.cm.request.ProducerRecordWriteRequest;
import org.ekbana.server.common.cm.request.TopicCreateRequest;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Arrays;
import java.util.List;

public class KafkaClient {
    private final InetSocketAddress inetSocketAddress;
    private SocketChannel channel;

    public KafkaClient(String host,int port) {
        inetSocketAddress=new InetSocketAddress(host,port);
    }

    public void startClient() throws IOException {
        channel=SocketChannel.open(inetSocketAddress);
        channel.configureBlocking(true);
        System.out.println("Client Started");
        new Thread(()-> {
            try {
                read();
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }).start();
    }

    public void write(String  message) throws IOException, InterruptedException {
        ByteBuffer buffer=ByteBuffer.wrap(message.getBytes());
        channel.write(buffer);
        Thread.sleep(200);
        buffer.clear();
    }

    public void read() throws IOException, InterruptedException {
        while (channel.isConnected()) {
            System.out.println();
            ByteBuffer buffer = ByteBuffer.allocate(1025);
            final int read = channel.read(buffer);
            if (read==-1){
                break;
            }
            if (read > 0) {
                System.out.println(new String(buffer.array()));
            }
            buffer.flip();
        }

        System.exit(0);
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        KafkaClient kafkaClient=new KafkaClient("localhost",9999);
        kafkaClient.startClient();

        kafkaClient.write(new Gson().toJson(new AuthRequest()));


//        Thread.sleep(20000);
        kafkaClient.write(new Gson().toJson(new TopicCreateRequest("tweet-29500",1)));

//        Thread.sleep(5000);
//        kafkaClient.write(new Gson().toJson(new TopicDeleteRequest("tweet-19500")));

//        kafkaClient.write(new Gson().toJson(new ProducerConfigRequest("tweet-19500")));
//
//        List<String> records= Arrays.asList("hello","world","first record","second record");
//        final ProducerRecordWriteRequest producerRecordWriteRequest = new ProducerRecordWriteRequest();
//        producerRecordWriteRequest.setTopicName("tweet-19500");
//        producerRecordWriteRequest.setProducerRecords(records);
//
////        kafkaClient.write(new Gson().toJson(producerRecordWriteRequest));
//
//        for (int i=0;i<10;i++){
//            kafkaClient.write(new Gson().toJson(producerRecordWriteRequest));
//            Thread.sleep(1000);
//        }
    }
}
