package org.ekbana.server.cluster;

//@AllArgsConstructor
//@NoArgsConstructor
public class Cluster {
    private final Node[] nodes= new Node[]{new Node("localhost")};

    public Node[] getPartitionNodes(int numberOfPartitions){
        Node[] dataNodes=new Node[numberOfPartitions];
        for (int i=0;i<numberOfPartitions;i++){
            dataNodes[i]=nodes[i%nodes.length];
        }
        return dataNodes;
    }

    public Node[] getReplicaNode(Node node,int numberOfReplicas){
        return new Node[numberOfReplicas];
    }
}
