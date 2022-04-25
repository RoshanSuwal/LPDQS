package org.ekbana.server.v2.client;

import com.google.gson.Gson;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.ekbana.server.common.KafkaServer;
import org.ekbana.server.common.cm.response.KafkaClientResponse;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

@AllArgsConstructor
@Getter
public class KafkaClient implements KafkaServer.KafkaServerClient {
    private final SocketChannel socketChannel;
    private KafkaClientState kafkaClientState;

    public void send(KafkaClientResponse kafkaClientResponse){
        try {
            socketChannel.write(ByteBuffer.wrap(new Gson().toJson(kafkaClientResponse).getBytes()));
        } catch (IOException e) {
            e.printStackTrace();
        }

        if (kafkaClientState==KafkaClientState.CLOSE){
            try {
                socketChannel.close();
                socketChannel.socket().close();
            } catch (IOException e) {
                e.printStackTrace();
            }

            kafkaClientState=KafkaClientState.CLOSED;
        }
    }

    public synchronized void setKafkaClientState(KafkaClientState kafkaClientState) {
        this.kafkaClientState = kafkaClientState;
    }
}
