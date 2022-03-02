package org.ekbana.minikafka.common;

import lombok.Getter;

import java.util.Objects;

@Getter
public class Node {
    private final String id;
    private final String hashId;
    private final String ipAddress;
    private final int weight;

    public Node(String id,String ipAddress){
        this(id,ipAddress,"");
    }

    public Node(String id,String ipAddress,String hashId){
        this(id,ipAddress,hashId,1);
    }

    public Node(String id,String ipAddress,String  hashId,int weight){
        this.id=id;
        this.ipAddress=ipAddress;
        this.weight=weight;
        this.hashId=hashId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Node node = (Node) o;
        return id.equals(node.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}
