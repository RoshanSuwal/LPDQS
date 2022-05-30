package org.ekbana.server.common;

import org.ekbana.minikafka.common.MessageParser;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ExecutorService;

public abstract class ClientSocket {
    private final InetSocketAddress inetSocketAddress;
    private SocketChannel socketChannel;
    private int BUFFER_SIZE=Integer.parseInt(System.getProperty("bufferSize","1048576"));

    public ClientSocket(String address,int port){
        this.inetSocketAddress=new InetSocketAddress(address,port);
    }

    public void  connect(ExecutorService executorService) throws IOException {
        socketChannel=SocketChannel.open(inetSocketAddress);
        socketChannel.configureBlocking(false);
        onStart(socketChannel);

//        if (executorService==null){
//            new Thread(this::read).start();
//        }else {
//            executorService.execute(this::read);
//        }

        read();
    }

    public void write(byte[] bytes) throws IOException {
//        System.out.println("sending to leader");
        final ByteBuffer byteBuffer = ByteBuffer.wrap(new MessageParser().encode(bytes));
//        System.out.println("[send to leader] msg :"+bytes.length+" | encoded :"+byteBuffer.array().length);
        socketChannel.write(byteBuffer);
        byteBuffer.clear();
    }

    private void read() throws ConnectException {
        try {
            MessageParser messageParser=new MessageParser();
            while (socketChannel.isConnected()) {
                ByteBuffer byteBuffer = ByteBuffer.allocate(BUFFER_SIZE);
                final int readByteCount = socketChannel.read(byteBuffer);
                byteBuffer.flip();
                if (readByteCount == -1) {
                    break;
                } else if (readByteCount > 0) {
                    byte[] readByte=new byte[readByteCount];
                    System.arraycopy(byteBuffer.array(),0,readByte,0,readByteCount);
                    messageParser.parse(readByte);
                    if (messageParser.hasReadAllBytes()){
                        onRead(messageParser.messageBytes());
                        messageParser=new MessageParser();
                    }
                }
                byteBuffer.clear();

//                try {
//                    Thread.sleep(10);
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
            }
        }catch (IOException e) {
            e.printStackTrace();
        }finally {
            close();
        }

        throw new ConnectException("socket connection closed");
    }

    public void close(){
        try {
            socketChannel.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        onClose();
//        System.exit(0);
    }

    protected abstract void onStart(SocketChannel socketChannel);
    protected abstract void onClose();

    protected abstract void onRead(byte[] readBytes);
}
