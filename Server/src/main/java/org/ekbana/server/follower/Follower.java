package org.ekbana.server.follower;

import org.ekbana.server.common.ClientSocket;
import org.ekbana.server.leader.KafkaServerConfig;

import java.nio.channels.SocketChannel;

public class Follower extends ClientSocket {
    private KafkaServerConfig kafkaServerConfig;
    private FollowerController.Listener<byte[]> listener;

    public Follower(KafkaServerConfig kafkaServerConfig) {
        super(kafkaServerConfig.getServer_address(), kafkaServerConfig.getPort());
        this.kafkaServerConfig=kafkaServerConfig;
    }

    public void registerListener(FollowerController.Listener<byte[]> listener){
        this.listener=listener;
    }

    @Override
    protected void onStart(SocketChannel socketChannel) {
        System.out.println("Follower connected to "+ socketChannel.socket().getRemoteSocketAddress());
    }

    @Override
    protected void onClose() {

    }

    @Override
    protected void onRead(byte[] readBytes) {
//        System.out.println("read bytes" + readBytes.length);
        if (listener!=null) listener.onListen(readBytes);
    }
}
