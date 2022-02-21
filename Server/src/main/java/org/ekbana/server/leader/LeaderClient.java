package org.ekbana.server.leader;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

@AllArgsConstructor
@Getter @Setter
public class LeaderClient {

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
    private final LeaderClientState leaderClientState;

    private final Node node;

    public void send(byte[] bytes){
        try {
            socketChannel.write(ByteBuffer.wrap(bytes));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
