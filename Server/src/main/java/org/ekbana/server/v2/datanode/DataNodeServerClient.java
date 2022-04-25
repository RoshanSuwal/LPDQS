package org.ekbana.server.v2.datanode;

import org.ekbana.server.common.ClientSocket;
import org.ekbana.server.config.KafkaProperties;

import java.nio.channels.SocketChannel;

public class DataNodeServerClient extends ClientSocket{

    enum NodeState{
        CONNECTED,NOT_CONNECTED,CLOSED,CONFIGURED
    }
    private KafkaProperties kafkaProperties;
    private NodeState nodeState;

    private DataNodeController.Receiver<byte[]> receiver;

    public void setReceiver(DataNodeController.Receiver receiver){
        this.receiver=receiver;
    }

    public DataNodeServerClient(KafkaProperties kafkaProperties){
        this(kafkaProperties.getKafkaProperty("kafka.server.address"), Integer.parseInt(kafkaProperties.getKafkaProperty("kafka.server.port")));
        this.kafkaProperties=kafkaProperties;
        this.nodeState=NodeState.NOT_CONNECTED;
    }

    public DataNodeServerClient(String address, int port) {
        super(address, port);
    }

    public void setNodeState(NodeState nodeState){
        this.nodeState=nodeState;
    }

    public NodeState getNodeState(){
        return this.nodeState;
    }

    @Override
    protected void onStart(SocketChannel socketChannel) {
        System.out.println("Data node connected to "+socketChannel.socket().getRemoteSocketAddress());
    }

    @Override
    protected void onClose() {
        System.out.println("Socket Connection Closed");
    }

    @Override
    protected void onRead(byte[] readBytes) {
        if (receiver!=null) receiver.onReceive(readBytes);
    }
}
