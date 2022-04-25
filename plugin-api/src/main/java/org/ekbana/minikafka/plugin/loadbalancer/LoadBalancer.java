package org.ekbana.minikafka.plugin.loadbalancer;


public interface LoadBalancer<T,R> {
    /*
    *  T -> Node object type, used to specify the node
    *  R -> request object used to find the node
    * */
    void addNode(T t);
    void removeNode(T t);
    T getAssignedNodeId(R r);

    int getAssignedNodePartitionId(R r);
}
