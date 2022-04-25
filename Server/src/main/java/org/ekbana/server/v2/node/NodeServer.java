package org.ekbana.server.v2.node;

import org.ekbana.minikafka.common.Node;
import org.ekbana.server.common.KafkaServer;
import org.ekbana.server.common.l.LFRequest;
import org.ekbana.server.config.KafkaProperties;
import org.ekbana.server.leader.LeaderClient;
import org.ekbana.server.leader.LeaderClientState;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.channels.SocketChannel;

public class NodeServer implements KafkaServer.KafkaServerListener {

    private KafkaProperties kafkaProperties;
    private NodeController nodeController;

    public NodeServer(KafkaProperties kafkaProperties, NodeController nodeController) {
        this.kafkaProperties = kafkaProperties;
        this.nodeController = nodeController;
    }

    @Override
    public String serverName() {
        return "Node server";
    }

    @Override
    public int port() {
        return 9998;
    }

    @Override
    public void onStart() {
        System.out.println("Node server started at port "+port());
    }

    @Override
    public void onRead(KafkaServer.KafkaServerClient kafkaServerClient, byte[] bytes) {
        nodeController.processTransactionFromNode((LeaderClient) kafkaServerClient,bytes);
    }

    @Override
    public void onConnectionClose(KafkaServer.KafkaServerClient kafkaServerClient) {
        System.out.println("node connection closed");
        nodeController.unRegisterNode((LeaderClient) kafkaServerClient);
    }

    @Override
    public void onConnectionCreated(KafkaServer.KafkaServerClient kafkaServerClient) {
        System.out.println("new node connected");
        nodeController.processNodeConfiguration(
                (LeaderClient) kafkaServerClient,new LFRequest(LFRequest.LFRequestType.NEW)
        );
    }

    @Override
    public KafkaServer.KafkaServerClient createAttachment(SocketChannel socketChannel) throws IOException {
        final SocketAddress remoteAddress = socketChannel.getRemoteAddress();
        return new LeaderClient(socketChannel, LeaderClientState.NEW,new Node(remoteAddress.toString(),remoteAddress.toString()));
    }
}
