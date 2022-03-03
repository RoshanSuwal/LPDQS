package org.ekbana.server.client;

import org.ekbana.server.common.KafkaServer;
import org.ekbana.server.common.ServerSocket;
import org.ekbana.server.common.cm.request.CloseClientRequest;
import org.ekbana.server.common.cm.request.NewConnectionRequest;
import org.ekbana.server.config.KafkaProperties;

import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

public class KafkaClientServer implements KafkaServer.KafkaServerListener {

    private final KafkaProperties kafkaProperties;
    private final KafkaClientController kafkaClientController;

    public KafkaClientServer(KafkaProperties kafkaProperties, KafkaClientController kafkaClientController) {
//        super(kafkaClientConfig.getAddress(),kafkaClientConfig.getPort());
        this.kafkaProperties = kafkaProperties;
        this.kafkaClientController = kafkaClientController;
    }

    @Override
    public String serverName() {
        return "Kafka Client server";
    }

    @Override
    public int port() {
        return Integer.parseInt(kafkaProperties.getKafkaProperty("kafka.client.server.port"));
    }

    @Override
    public void onStart() {
        System.out.println("Kafka Client Server started at port "+port());
    }

    @Override
    public void onRead(KafkaServer.KafkaServerClient kafkaServerClient, byte[] bytes) {
        kafkaClientController.rawRequest((KafkaClient) kafkaServerClient,bytes);
    }

    @Override
    public void onConnectionClose(KafkaServer.KafkaServerClient kafkaServerClient) {
        System.out.println("connection closed");
        kafkaClientController.request((KafkaClient) kafkaServerClient,new CloseClientRequest());
    }

    @Override
    public void onConnectionCreated(KafkaServer.KafkaServerClient kafkaServerClient) {
        System.out.println("new connection created");
        kafkaClientController.request((KafkaClient) kafkaServerClient,new NewConnectionRequest());
    }

    @Override
    public KafkaServer.KafkaServerClient createAttachment(SocketChannel socketChannel) {
        return new KafkaClient(socketChannel,KafkaClientState.NEW);
    }

//    @Override
//    protected KafkaClient createAttachment(SocketChannel socketChannel) {
//        return new KafkaClient(socketChannel,KafkaClientState.NEW);
//    }
//
//    @Override
//    protected void onConnectionCreated(KafkaClient kafkaClient) {
//        System.out.println("new connection created");
//        kafkaClientController.request(kafkaClient,new NewConnectionRequest());
//    }
//
//    @Override
//    protected void onConnectionClose(KafkaClient kafkaClient) {
//        System.out.println("connection closed");
//        kafkaClientController.request(kafkaClient,new CloseClientRequest());
//    }
//
//    @Override
//    protected void onRead(KafkaClient kafkaClient, byte[] bytes) {
//        kafkaClientController.rawRequest(kafkaClient,bytes);
//    }
//
//    @Override
//    protected void onStart(int port) {
//        System.out.println("KafkaClientServer Started at port "+kafkaClientConfig.getPort());
//    }

}
