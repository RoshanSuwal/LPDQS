package org.ekbana.server.util;

import lombok.Getter;
import org.ekbana.server.cluster.Node;

import java.util.Arrays;

@Getter
public class MiniKafkaConfig {
    private final int clientServerPort=9999;
    private final int transportLayerPort=9998;

    private final int CLIENT_REQUEST_QUEUE_MAX_SIZE=2;
    private final String nodeAddress="localhost";
    private final String nodes="localhost";
    private final String masterAddress="localhost";

    public Node[] getNodes(){
        return (Node[]) Arrays.stream(nodes.split(",")).map(Node::new).toArray();
    }

    public Node getNode(){
        return new Node(nodeAddress);
    }

    public Node getMasterNode(){
        return new Node(masterAddress);
    }
}
