package org.ekbana.minikafka.client.common;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public  abstract class KafkaServerClient {

    public enum ServerState{
        NOT_CONNECTED,CONNECTING,CONNECTED,
        AUTHETICATING,AUTHENTICATED,
        CONIGURING,CONFIGURED,
        CLOSE
    }

    private final InetSocketAddress inetSocketAddress;
    private SocketChannel socketChannel;

    public KafkaServerClient(String address,int port) {
        inetSocketAddress=new InetSocketAddress(address,port);
    }

    public void connect() throws IOException {
        socketChannel=SocketChannel.open();
        socketChannel.configureBlocking(true);
        socketChannel.connect(inetSocketAddress);
        socketChannel.finishConnect();

        new Thread(()-> {
            try {
                read();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }).start();
    }

    public void write(String  message) throws IOException, InterruptedException {
        ByteBuffer buffer=ByteBuffer.wrap(message.getBytes());
        socketChannel.write(buffer);
        onSend(message);
        Thread.sleep(200);
        buffer.clear();
    }

    private void read() throws IOException {
        while (socketChannel.isConnected()) {
            ByteBuffer buffer = ByteBuffer.allocate(128*1024);// fixed size
            final int read = socketChannel.read(buffer);
            if (read==-1){
                break;
            }
            if (read > 0) {
                byte[] readByte=new byte[read];
                System.arraycopy(buffer.array(),0,readByte,0,read);
                onRead(new String(readByte).trim());
            }
            buffer.flip();
        }
        close();
    }

    protected void close(){
        try {
            socketChannel.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        onClose();
    }

    protected abstract void onConnect();
    protected abstract void onRead(String readData);
    protected abstract void onClose();
    protected abstract void onSend(String message);
}
