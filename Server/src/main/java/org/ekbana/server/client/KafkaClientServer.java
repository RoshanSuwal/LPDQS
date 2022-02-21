package org.ekbana.server.client;

import org.ekbana.server.common.ServerSocket;
import org.ekbana.server.common.cm.request.CloseClientRequest;
import org.ekbana.server.common.cm.request.NewConnectionRequest;

import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

public class KafkaClientServer extends ServerSocket<KafkaClient> {

    private final KafkaClientConfig kafkaClientConfig;
    private final KafkaClientController kafkaClientController;

    public KafkaClientServer(KafkaClientConfig kafkaClientConfig, KafkaClientController kafkaClientController) {
        super(kafkaClientConfig.getAddress(),kafkaClientConfig.getPort());
        this.kafkaClientConfig = kafkaClientConfig;
        this.kafkaClientController = kafkaClientController;
    }

    @Override
    protected KafkaClient createAttachment(SocketChannel socketChannel) {
        return new KafkaClient(socketChannel,KafkaClientState.NEW);
    }

    @Override
    protected void onConnectionCreated(KafkaClient kafkaClient) {
        System.out.println("new connection created");
        kafkaClientController.request(kafkaClient,new NewConnectionRequest());
    }

    @Override
    protected void onConnectionClose(KafkaClient kafkaClient) {
        System.out.println("connection closed");
        kafkaClientController.request(kafkaClient,new CloseClientRequest());
    }

    @Override
    protected void onRead(KafkaClient kafkaClient, byte[] bytes) {
        kafkaClientController.rawRequest(kafkaClient,bytes);
    }

    @Override
    protected void onStart() {
        System.out.println("KafkaClientServer Started at port "+kafkaClientConfig.getPort());
    }

}
