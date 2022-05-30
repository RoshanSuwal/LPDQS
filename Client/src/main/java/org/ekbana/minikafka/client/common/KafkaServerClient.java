package org.ekbana.minikafka.client.common;

import org.ekbana.minikafka.common.MessageParser;

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
    private int BUFFER_SIZE=Integer.parseInt(System.getProperty("bufferSize","1048576"));

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

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public void write(String  message) throws IOException, InterruptedException {
//        Thread.sleep(100);
        ByteBuffer buffer=ByteBuffer.wrap(new MessageParser().encode(message.getBytes()));
        socketChannel.write(buffer);
        onSend(message);
        buffer.clear();
        Thread.sleep(200);
    }

    private void read() throws IOException {
        MessageParser messageParser=new MessageParser();
        while (socketChannel.isConnected()) {
//            System.out.println("BUFFER_SIZE:"+BUFFER_SIZE);
            ByteBuffer buffer = ByteBuffer.allocate(10);// fixed size
//            System.out.println(buffer.reset());
            buffer.clear();
//            System.out.println("[OnRead] buffer pos 0 : "+buffer.position());
            final int read = socketChannel.read(buffer);
//            System.out.println("[OnRead] buffer size :"+read+ " capacity :"+ buffer.remaining());
            buffer.flip();
//            System.out.println("[OnRead] buffer pos 1: "+buffer.position());
            if (read==-1){
                break;
            }
            if (read > 0) {
                byte[] readByte=new byte[read];
                System.arraycopy(buffer.array(),0,readByte,0,read);
                messageParser.parse(readByte);
                if (messageParser.hasReadAllBytes()){
                    onRead(new String(messageParser.messageBytes()));
                    messageParser=new MessageParser();
                }
            }
            buffer.clear();
        }
        System.out.println("read thread closed");
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
