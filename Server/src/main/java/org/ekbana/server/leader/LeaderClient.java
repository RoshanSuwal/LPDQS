package org.ekbana.server.leader;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import org.ekbana.minikafka.common.Node;
import org.ekbana.server.common.KafkaServer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

@AllArgsConstructor
@Getter @Setter
public class LeaderClient implements KafkaServer.KafkaServerClient {

    // has 3 roles
    // 1. ClientServerTransportClient
    // 2. BrokerTransportClient
    // 3. ReplicaTransportClient
    //
    //           Request
    //              |
    //    Request/Response Processor
    //      |        |       |
    //     CTC      BTC     RTC

    private final SocketChannel socketChannel;
    private LeaderClientState leaderClientState;

    private Node node;

    public synchronized void send(byte[] bytes){
        try {
            socketChannel.write(ByteBuffer.wrap(bytes));
            Thread.sleep(100);
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

}
