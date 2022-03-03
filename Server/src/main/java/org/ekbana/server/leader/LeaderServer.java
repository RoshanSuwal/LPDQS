package org.ekbana.server.leader;

import org.ekbana.server.cluster.Node;
import org.ekbana.server.common.KafkaServer;
import org.ekbana.server.common.l.LFRequest;
import org.ekbana.server.config.KafkaProperties;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.channels.SocketChannel;

public class LeaderServer implements KafkaServer.KafkaServerListener {//extends ServerSocket<LeaderClient> {

    private  final KafkaProperties kafkaProperties;
    private final LeaderController leaderController;

    public LeaderServer(KafkaProperties kafkaProperties, LeaderController leaderController) {
//        super(kafkaServerConfig.getServer_address(), kafkaServerConfig.getPort());
        this.kafkaProperties=kafkaProperties;
        this.leaderController = leaderController;
    }

    @Override
    public int port(){
        return Integer.parseInt(kafkaProperties.getKafkaProperty("kafka.server.port"));
    }

    @Override
    public String serverName() {
        return "leader";
    }

    @Override
    public void onStart() {
        System.out.println("leader server started at port "+port());
    }

    @Override
    public void onRead(KafkaServer.KafkaServerClient kafkaServerClient, byte[] bytes) {
        leaderController.rawData((LeaderClient) kafkaServerClient,bytes);
    }

    @Override
    public void onConnectionClose(KafkaServer.KafkaServerClient kafkaServerClient) {
        System.out.println("follower connection closed by client with address "+ ((LeaderClient)kafkaServerClient).getSocketChannel().socket().getRemoteSocketAddress());
        leaderController.removeNode((LeaderClient) kafkaServerClient);
    }

    @Override
    public void onConnectionCreated(KafkaServer.KafkaServerClient kafkaServerClient) {
        System.out.println("new follower connected with address "+ ((LeaderClient)kafkaServerClient).getSocketChannel().socket().getRemoteSocketAddress());
//        leaderController.addNode((LeaderClient) kafkaServerClient);
        leaderController.processFollowerRequest((LeaderClient)kafkaServerClient,new LFRequest(LFRequest.LFRequestType.NEW));
        // send auth request to client
    }

    @Override
    public KafkaServer.KafkaServerClient createAttachment(SocketChannel socketChannel) throws IOException {
        final SocketAddress remoteAddress = socketChannel.getRemoteAddress();
        return new LeaderClient(socketChannel, LeaderClientState.NEW,new Node(remoteAddress.toString(),remoteAddress.toString()));
    }

//    @Override
//    protected LeaderClient createAttachment(SocketChannel socketChannel) throws IOException {
//        final SocketAddress remoteAddress = socketChannel.getRemoteAddress();
//        return new LeaderClient(socketChannel, LeaderClientState.NEW,new Node(remoteAddress.toString()));
//    }
//
//    @Override
//    protected void onConnectionCreated(LeaderClient leaderClient) {
//        System.out.println("new follower connected with address "+ leaderClient.getSocketChannel().socket().getRemoteSocketAddress());
//        leaderController.addNode(leaderClient);
//    }
//
//    @Override
//    protected void onConnectionClose(LeaderClient leaderClient) {
//        System.out.println("follower connection closed by client with address "+ leaderClient.getSocketChannel().socket().getRemoteSocketAddress());
//        leaderController.removeNode(leaderClient);
//    }
//
//    @Override
//    protected void onRead(LeaderClient leaderClient, byte[] bytes) {
//        leaderController.rawData(leaderClient,bytes);
//    }
//
//    @Override
//    protected void onStart(int port) {
//        System.out.println("Leader server started at port "+ kafkaServerConfig.getPort());
//    }

    // handles Transport clients : ClientServerTransportClient & Broker Client
    // 3 - types of request processing
    // 1. Client Request- Response
    // 2. Broker Request-Response
    // 3. replica Request-Response

    // simply redirects the the request to targeted nodes
    // no io and cpu intensive tasks

}
