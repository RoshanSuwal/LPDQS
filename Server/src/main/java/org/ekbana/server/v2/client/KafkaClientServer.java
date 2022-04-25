package org.ekbana.server.v2.client;

import org.ekbana.server.common.KafkaServer;
import org.ekbana.server.common.cm.request.CloseClientRequest;
import org.ekbana.server.common.cm.request.NewConnectionRequest;
import org.ekbana.server.config.KafkaProperties;

import java.io.IOException;
import java.nio.channels.SocketChannel;

public class KafkaClientServer implements KafkaServer.KafkaServerListener {

    private final KafkaProperties kafkaProperties;
    private final KafkaClientController kafkaClientController;

    public KafkaClientServer(KafkaProperties kafkaProperties, KafkaClientController kafkaClientController) {
        this.kafkaProperties = kafkaProperties;
        this.kafkaClientController = kafkaClientController;
    }

    @Override
    public String serverName() {
        return "Kafka Client Server";
    }

    @Override
    public int port() {
        return Integer.parseInt(kafkaProperties.getKafkaProperty("kafka.client.server.port"));
    }

    @Override
    public void onStart() {
        System.out.println("Kafka Client server started at port  "+port());
    }

    @Override
    public void onRead(KafkaServer.KafkaServerClient kafkaServerClient, byte[] bytes) {
        kafkaClientController.request((KafkaClient) kafkaServerClient,bytes);
    }

    @Override
    public void onConnectionClose(KafkaServer.KafkaServerClient kafkaServerClient) {
        kafkaClientController.request((KafkaClient) kafkaServerClient,new CloseClientRequest());
    }

    @Override
    public void onConnectionCreated(KafkaServer.KafkaServerClient kafkaServerClient) {
        System.out.println("New client connection received");
        kafkaClientController.request((KafkaClient) kafkaServerClient,new NewConnectionRequest());
    }

    @Override
    public KafkaServer.KafkaServerClient createAttachment(SocketChannel socketChannel) throws IOException {
        return new KafkaClient(socketChannel,KafkaClientState.NEW);
    }
}
