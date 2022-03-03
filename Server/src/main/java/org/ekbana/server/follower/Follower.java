package org.ekbana.server.follower;

import org.ekbana.server.common.ClientSocket;
import org.ekbana.server.config.KafkaProperties;

import java.nio.channels.SocketChannel;

public class Follower extends ClientSocket {
    enum FollowerState {
        NOT_CONNECTED, CONNECTED, AUTHENTICATED,CLOSE
    }

    private KafkaProperties kafkaProperties;
    private FollowerController.Listener<byte[]> listener;
    private FollowerState followerState;

    public Follower(KafkaProperties kafkaProperties) {
        super(kafkaProperties.getKafkaProperty("kafka.server.address"), Integer.parseInt(kafkaProperties.getKafkaProperty("kafka.server.port")));
        this.kafkaProperties = kafkaProperties;
        this.followerState = FollowerState.NOT_CONNECTED;
    }

    public FollowerState getFollowerState() {
        return followerState;
    }

    public void setFollowerState(FollowerState followerState) {
        this.followerState = followerState;
    }

    public void registerListener(FollowerController.Listener<byte[]> listener) {
        this.listener = listener;
    }

    @Override
    protected void onStart(SocketChannel socketChannel) {
        System.out.println("Follower connected to " + socketChannel.socket().getRemoteSocketAddress());
    }

    @Override
    protected void onClose() {
        System.out.println("Socket Connection Closed");
    }

    @Override
    protected void onRead(byte[] readBytes) {
        if (listener != null) listener.onListen(readBytes);

//        System.out.println("read bytes" + readBytes.length);
//        if (listener!=null) listener.onListen(readBytes);
    }
}
