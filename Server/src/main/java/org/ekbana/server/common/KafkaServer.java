package org.ekbana.server.common;

import org.ekbana.server.util.Mapper;

import java.io.IOException;
import java.nio.channels.SocketChannel;

public class KafkaServer extends ServerSocket<KafkaServer.KafkaServerListener> {

    private final Mapper<Integer,KafkaServerListener> serverListenerMapper=new Mapper<>();

    public void register(int port,KafkaServerListener kafkaServerListener){
        serverListenerMapper.add(port,kafkaServerListener);
    }

    public void start() throws IOException {
        this.startServer(serverListenerMapper.getKeys().stream().mapToInt(key->key).toArray());
    }

    @Override
    protected KafkaServerListener getPortListener(int port){
        return serverListenerMapper.get(port);
    }

    public interface KafkaServerListener{
        String serverName();
        int port();
        void onStart();
        void onRead(KafkaServerClient kafkaServerClient,byte[] bytes);
        void onConnectionClose(KafkaServerClient kafkaServerClient);
        void onConnectionCreated(KafkaServerClient kafkaServerClient);
        KafkaServerClient createAttachment(SocketChannel socketChannel) throws IOException;
    }

    public interface KafkaServerClient{

    }
}
