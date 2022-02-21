package org.ekbana.server.common;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ExecutorService;

public abstract class ClientSocket {
    private final InetSocketAddress inetSocketAddress;
    private SocketChannel socketChannel;

    public ClientSocket(String address,int port){
        this.inetSocketAddress=new InetSocketAddress(address,port);
    }

    public void  connect(ExecutorService executorService) throws IOException {
        socketChannel=SocketChannel.open(inetSocketAddress);
        socketChannel.configureBlocking(false);
        onStart(socketChannel);

        if (executorService==null){
            new Thread(this::read).start();
        }else {
            executorService.execute(this::read);
        }
    }

    public void write(byte[] bytes) throws IOException {
//        System.out.println("sending to leader");
        final ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        socketChannel.write(byteBuffer);
        byteBuffer.clear();
    }

    private void read(){
        try {
            while (socketChannel.isConnected()) {
                ByteBuffer byteBuffer = ByteBuffer.allocate(1024*64);
                final int readByteCount = socketChannel.read(byteBuffer);
                byteBuffer.flip();
                if (readByteCount == -1) {
                    break;
                } else if (readByteCount > 0) {
                    byte[] readByte=new byte[readByteCount];
                    System.arraycopy(byteBuffer.array(),0,readByte,0,readByteCount);
                    onRead(readByte);
                }
                byteBuffer.clear();

                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }catch (IOException e) {
            e.printStackTrace();
        }finally {
            close();
        }
    }

    private void close(){
        onClose();
        try {
            socketChannel.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    protected abstract void onStart(SocketChannel socketChannel);
    protected abstract void onClose();

    protected abstract void onRead(byte[] readBytes);
}
