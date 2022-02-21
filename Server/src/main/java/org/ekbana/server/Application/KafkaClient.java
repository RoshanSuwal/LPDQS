package org.ekbana.server.Application;

import com.google.gson.Gson;
import org.ekbana.server.common.cm.request.AuthRequest;
import org.ekbana.server.common.cm.request.TopicCreateRequest;
import org.ekbana.server.common.cm.request.TopicDeleteRequest;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class KafkaClient {
    private final InetSocketAddress inetSocketAddress;
    private SocketChannel channel;

    public KafkaClient(String host,int port) {
        inetSocketAddress=new InetSocketAddress(host,port);
    }

    public void startClient() throws IOException {
        channel=SocketChannel.open(inetSocketAddress);
        channel.configureBlocking(true);
        System.out.println("Client Started");
        new Thread(()-> {
            try {
                read();
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }).start();
    }

    public void write(String  message) throws IOException {
        ByteBuffer buffer=ByteBuffer.wrap(message.getBytes());
        channel.write(buffer);
        buffer.clear();
    }

    public void read() throws IOException, InterruptedException {
        while (channel.isConnected()) {
            System.out.println();
            ByteBuffer buffer = ByteBuffer.allocate(1025);
            final int read = channel.read(buffer);
            if (read==-1){
                break;
            }
            if (read > 0) {
                System.out.println(new String(buffer.array()));
            }
            buffer.flip();
        }

        System.exit(0);
    }

    public static void main(String[] args) throws IOException {
        KafkaClient kafkaClient=new KafkaClient("localhost",9999);
        kafkaClient.startClient();

        kafkaClient.write(new Gson().toJson(new AuthRequest()));
        kafkaClient.write(new Gson().toJson(new TopicCreateRequest("tweet",1)));
//        kafkaClient.write(new Gson().toJson(new TopicDeleteRequest("test")));
    }

}
