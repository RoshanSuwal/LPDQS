package org.ekbana.server.network;

import lombok.AllArgsConstructor;
import org.ekbana.server.entity.request.*;
import org.ekbana.server.util.MiniKafkaConfig;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.*;

@AllArgsConstructor
public class ServerSocket {

    private final ServerSocketCallback serverSocketCallback;
    private final MiniKafkaConfig miniKafkaConfig;
    private final RequestProcessor requestProcessor;

    private Selector selector;
    private final InetSocketAddress address;
    private final Map<SocketChannel, List> dataMapper;

    private final RequestParser requestParser=new RequestParser();

    public ServerSocket(String hostAddress,int port,ServerSocketCallback serverSocketCallback){
        this.address=new InetSocketAddress(hostAddress,port);
        this.miniKafkaConfig=null;
        this.requestProcessor=null;
        this.dataMapper = new HashMap<SocketChannel, List>();
        this.serverSocketCallback=serverSocketCallback;
    }

    public ServerSocket(MiniKafkaConfig miniKafkaConfig,RequestProcessor requestProcessor){
        this.miniKafkaConfig=miniKafkaConfig;
        this.requestProcessor=requestProcessor;
        this.address=new InetSocketAddress(miniKafkaConfig.getNodeAddress(),miniKafkaConfig.getClientServerPort());
        this.dataMapper = new HashMap<SocketChannel, List>();
        this.serverSocketCallback=null;
    }

    public void startServer() throws IOException {
        this.selector=Selector.open();
        ServerSocketChannel serverSocketChannel=ServerSocketChannel.open();
        serverSocketChannel.configureBlocking(false);
        serverSocketChannel.socket().bind(address);
        serverSocketChannel.register(this.selector, SelectionKey.OP_ACCEPT);

        System.out.println("Server Started ...");

        while (true){
            this.selector.select();
            Iterator<SelectionKey> keys=this.selector.selectedKeys().iterator();

            while (keys.hasNext()){
                SelectionKey key= (SelectionKey) keys.next();

                keys.remove();

                if (!key.isValid())continue;
                if (key.isAcceptable()) accept(key);
                else if (key.isReadable()) read(key);
            }
        }
    }

    private void read(SelectionKey key) throws IOException {
        System.out.println(key.attachment());
        SocketChannel channel=(SocketChannel) key.channel();
        ByteBuffer buffer=ByteBuffer.allocate(1024);
        int numRead=-1;
        numRead=channel.read(buffer);

        buffer.flip();
        if (numRead==-1){
            dataMapper.remove(channel);
            Socket socket=channel.socket();
            SocketAddress remoteAddress=socket.getRemoteSocketAddress();
            System.out.println("Connection closed by client: "+remoteAddress);
            requestProcessor.process(new CloseConnectionRequest(),(MiniKafkaClient) key.attachment());
//            ((KClient)key.attachment()).stop();
            serverSocketCallback.close(key);
            channel.close();
            socket.close();
            key.cancel();
            return;
        }

        requestProcessor.process(requestParser.parse(buffer,numRead),(MiniKafkaClient) key.attachment());

//        ((KClient)key.attachment()).registerRequest(requestParser.parse(buffer,numRead));

//        byte[] data=new byte[numRead];
//        System.arraycopy(buffer.array(),0,data,0,numRead);
//        System.out.println("Got: "+new String(data));
        buffer.clear();

//        new Thread(()->echo(new String(data),channel)).start();
    }

    public void echo(String data,SocketChannel channel){
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        try {
            channel.write(ByteBuffer.wrap(data.getBytes()));
//            channel.register(this.selector,SelectionKey.OP_WRITE);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private void accept(SelectionKey key) throws IOException {
        ServerSocketChannel serverChannel=(ServerSocketChannel) key.channel();
        SocketChannel socketChannel=serverChannel.accept();
        socketChannel.configureBlocking(false);
        Socket socket=socketChannel.socket();
        SocketAddress remoteAddress=socket.getRemoteSocketAddress();
        System.out.println("connected to: "+remoteAddress);

        dataMapper.put(socketChannel,new ArrayList());
//        socketChannel.register(this.selector,SelectionKey.OP_READ,new KClient(socketChannel,miniKafkaConfig,executorServie));
        final MiniKafkaClient miniKafkaClient = new MiniKafkaClient(miniKafkaConfig, socketChannel, MiniKafkaClient.KafkaClientState.NEW);
        socketChannel.register(this.selector,SelectionKey.OP_READ, miniKafkaClient);
        requestProcessor.process(new BaseRequest(Request.RequestType.NEW_CONNECTION),miniKafkaClient);

        serverSocketCallback.accept(key);
    }

//    public static void main(String[] args) throws IOException {
////        MiniKafkaConfig miniKafkaConfig=new MiniKafkaConfig();
////        ServerSocket serverSocket=new ServerSocket(miniKafkaConfig, "localhost",9999);
////        serverSocket.startServer();
//    }
}
