//package org.ekbana.server.Application;
//
//import com.google.gson.Gson;
//import org.ekbana.server.common.cm.request.*;
//
//import java.io.IOException;
//import java.net.InetSocketAddress;
//import java.nio.ByteBuffer;
//import java.nio.channels.SocketChannel;
//import java.time.Instant;
//import java.util.Arrays;
//import java.util.List;
//
//public class KafkaClientApplication {
//
//    public interface  KafkaHandler{
//         void read(String msg);
//    }
//
//    private final InetSocketAddress inetSocketAddress;
//    private SocketChannel channel;
//    private KafkaHandler kafkaHandler;
//
//    public KafkaClientApplication(String host, int port,KafkaHandler kafkaHandler) {
//        inetSocketAddress=new InetSocketAddress(host,port);
//        this.kafkaHandler=kafkaHandler;
//    }
//
//    public void startClient() throws IOException {
//        channel=SocketChannel.open(inetSocketAddress);
//        channel.configureBlocking(true);
//        System.out.println("Client Started");
//        new Thread(()-> {
//            try {
//                read();
//            } catch (IOException | InterruptedException e) {
//                e.printStackTrace();
//            }
//        }).start();
//    }
//
//    public void write(String  message) throws IOException, InterruptedException {
//        ByteBuffer buffer=ByteBuffer.wrap(message.getBytes());
//        channel.write(buffer);
//        Thread.sleep(200);
//        buffer.clear();
//    }
//
//    public void read() throws IOException, InterruptedException {
//        while (channel.isConnected()) {
//            System.out.println();
//            ByteBuffer buffer = ByteBuffer.allocate(1025);
//            final int read = channel.read(buffer);
//            if (read==-1){
//                break;
//            }
//            if (read > 0) {
////                System.out.println(new String(buffer.array()));
//                kafkaHandler.read(new String(buffer.array()));
//            }
//            buffer.flip();
//        }
//
//        System.exit(0);
//    }
//
//    public static void main(String[] args) throws IOException, InterruptedException {
//        KafkaHandler kafkaHandler=new KafkaHandler() {
//            @Override
//            public void read(String msg) {
//                System.out.println(msg);
//            }
//        };
//
//        KafkaClientApplication kafkaClient=new KafkaClientApplication("localhost",9999,kafkaHandler);
//        kafkaClient.startClient();
//
//        kafkaClient.write(new Gson().toJson(new AuthRequest()));
//
//
////        Thread.sleep(20000);
//        kafkaClient.write(new Gson().toJson(new TopicCreateRequest("tweets",2)));
//
////        Thread.sleep(5000);
////        kafkaClient.write(new Gson().toJson(new TopicDeleteRequest("tweet-19500")));
//
////        kafkaClient.write(new Gson().toJson(new ProducerConfigRequest("tweet-19500")));
////
////        List<String> records= Arrays.asList("hello","world","first record","second record");
////        final ProducerRecordWriteRequest producerRecordWriteRequest = new ProducerRecordWriteRequest();
////        producerRecordWriteRequest.setTopicName("tweet-19500");
////        producerRecordWriteRequest.setProducerRecords(records);
////
//////        kafkaClient.write(new Gson().toJson(producerRecordWriteRequest));
////
////        while (true){
////            kafkaClient.write(new Gson().toJson(producerRecordWriteRequest));
////            Thread.sleep(1000);
////        }
//
////        final ConsumerConfigRequest consumerConfigRequest = new ConsumerConfigRequest("tweet-19500", "group-0", new int[]{0});
////        kafkaClient.write(new Gson().toJson(consumerConfigRequest));
////
////        System.out.println("1645596567106");
////        System.out.println(Instant.now().toEpochMilli());
////        System.out.println(Instant.now().getEpochSecond());
////        Thread.sleep(200);
////        final ConsumerRecordReadRequest consumerRecordReadRequest = new ConsumerRecordReadRequest("tweet-19500", 0, 0, false);
////
//////        kafkaClient.write(new Gson().toJson(consumerRecordReadRequest));
////        kafkaClient.write(new Gson().toJson(new ConsumerRecordReadRequest("tweet-19500", 0, -1000, false)));
////        kafkaClient.write(new Gson().toJson(new ConsumerRecordReadRequest("tweet-19500", 0, 0, false)));
////        kafkaClient.write(new Gson().toJson(new ConsumerRecordReadRequest("tweet-19500", 0, 103, false)));
////        kafkaClient.write(new Gson().toJson(new ConsumerRecordReadRequest("tweet-19500", 0, 400, false)));
//    }
//}
