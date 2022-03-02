package org.ekbana.minikafka.plugin.loadbalancer;

public interface LoadBalancerFactory<P,T,R> {
    /*
     * P -> input parameter of LoadBalancerFactory , generally the objects needed to create the load balancer
     * T -> Type of input to register, remove node
     * R -> Type of request object that helps to define the desired node
     */

    /* gives the name of load balancer, must be unique*/
    String loadBalancerName();
    // returns the load balancer
    LoadBalancer<T,R> buildLoadBalancer(P p);
}
