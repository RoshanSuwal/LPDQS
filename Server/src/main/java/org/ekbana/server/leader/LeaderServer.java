package org.ekbana.server.leader;

import org.ekbana.server.common.ServerSocket;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

public class LeaderServer extends ServerSocket<LeaderClient> {

    private  final KafkaServerConfig kafkaServerConfig;
    private final LeaderController leaderController;

    public LeaderServer(KafkaServerConfig kafkaServerConfig, LeaderController leaderController) {
        super(kafkaServerConfig.getServer_address(), kafkaServerConfig.getPort());
        this.kafkaServerConfig=kafkaServerConfig;
        this.leaderController = leaderController;
    }

    @Override
    protected LeaderClient createAttachment(SocketChannel socketChannel) throws IOException {
        final SocketAddress remoteAddress = socketChannel.getRemoteAddress();
        return new LeaderClient(socketChannel, LeaderClientState.NEW,new Node(remoteAddress.toString()));
    }

    @Override
    protected void onConnectionCreated(LeaderClient leaderClient) {
        System.out.println("new follower connected with address "+ leaderClient.getSocketChannel().socket().getRemoteSocketAddress());
        leaderController.addNode(leaderClient);
    }

    @Override
    protected void onConnectionClose(LeaderClient leaderClient) {
        System.out.println("follower connection closed by client with address "+ leaderClient.getSocketChannel().socket().getRemoteSocketAddress());
    }

    @Override
    protected void onRead(LeaderClient leaderClient, byte[] bytes) {
        leaderController.rawData(leaderClient,bytes);
    }

    @Override
    protected void onStart() {
        System.out.println("Leader server started at port "+ kafkaServerConfig.getPort());
    }

    // handles Transport clients : ClientServerTransportClient & Broker Client
    // 3 - types of request processing
    // 1. Client Request- Response
    // 2. Broker Request-Response
    // 3. replica Request-Response

    // simply redirects the the request to targeted nodes
    // no io and cpu intensive tasks

}
