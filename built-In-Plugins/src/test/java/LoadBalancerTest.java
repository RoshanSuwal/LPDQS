import org.ekbana.minikafka.common.LBRequest;
import org.ekbana.minikafka.common.Mapper;
import org.ekbana.minikafka.common.Node;
import org.ekbana.minikafka.plugin.loadbalancer.LoadBalancer;
import org.ekbana.minikafka.plugins.factory.ConsistentHashingLoadBalancerFactory;
import org.ekbana.minikafka.plugins.factory.WeightedRoundRobinLoadBalancerFactory;
import org.junit.Assert;
import org.junit.Test;

public class LoadBalancerTest {

    @Test
    public void weightedRoundRobinTest(){
        Mapper<String, Node> mapper=new Mapper<>();
        mapper.add("node-0",new Node("node-0","localhost"));
        mapper.add("node-2",new Node("node-2","localhost"));
        mapper.add("node-1",new Node("node-1","localhost"));
        mapper.add("node-3",new Node("node-3","localhost"));
        final WeightedRoundRobinLoadBalancerFactory weightedRoundRobinLoadBalancerFactory = new WeightedRoundRobinLoadBalancerFactory();

        final LoadBalancer<Node, LBRequest> loadBalancer = weightedRoundRobinLoadBalancerFactory.buildLoadBalancer(mapper);
        loadBalancer.addNode(new Node("node-4","localhost","",2));
        mapper.forEach((id,node)->loadBalancer.addNode(node));
        mapper.add("node-4",new Node("node-4","localhost","",2));

        Assert.assertEquals("node-4",loadBalancer.getAssignedNodeId(LBRequest.builder().build()).getId());
        Assert.assertEquals("node-4",loadBalancer.getAssignedNodeId(LBRequest.builder().build()).getId());
        Assert.assertEquals("node-0",loadBalancer.getAssignedNodeId(LBRequest.builder().build()).getId());
        Assert.assertEquals("node-1",loadBalancer.getAssignedNodeId(LBRequest.builder().build()).getId());
        Assert.assertEquals("node-2",loadBalancer.getAssignedNodeId(LBRequest.builder().build()).getId());

        loadBalancer.removeNode(new Node("node-3","localhost"));

        Assert.assertEquals("node-4",loadBalancer.getAssignedNodeId(LBRequest.builder().build()).getId());
        Assert.assertEquals("node-4",loadBalancer.getAssignedNodeId(LBRequest.builder().build()).getId());
        Assert.assertEquals("node-0",loadBalancer.getAssignedNodeId(LBRequest.builder().build()).getId());

    }

    @Test
    public void consistentHashingTest(){
        Mapper<String, Node> mapper=new Mapper<>();
        mapper.add("node-0",new Node("node-0","localhost","10000",1));
        mapper.add("node-1",new Node("node-1","localhost","20000",1));
        mapper.add("node-2",new Node("node-2","localhost","30000",1));
        mapper.add("node-3",new Node("node-3","localhost","40000",1));

        final ConsistentHashingLoadBalancerFactory consistentHashingLoadBalancerFactory = new ConsistentHashingLoadBalancerFactory();
        final LoadBalancer<Node, LBRequest> loadBalancer = consistentHashingLoadBalancerFactory.buildLoadBalancer(mapper);

        mapper.forEach((id,node)->loadBalancer.addNode(node));
        Assert.assertEquals("node-0",loadBalancer.getAssignedNodeId(LBRequest.builder().id("123").build()).getId());
        Assert.assertEquals("node-3",loadBalancer.getAssignedNodeId(LBRequest.builder().id("31000").build()).getId());
        Assert.assertEquals("node-0",loadBalancer.getAssignedNodeId(LBRequest.builder().id("101000").build()).getId());



    }


}
