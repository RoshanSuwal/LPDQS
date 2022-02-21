package org.ekbana.server.leader;

import java.util.HashMap;

public class NodeClientMapper {
    private final HashMap<Node,LeaderClient> leaderClientHashMap=new HashMap<>();

    public void addNode(Node node,LeaderClient leaderClient){
        leaderClientHashMap.put(node,leaderClient);
    }

    public LeaderClient getLeaderClient(Node node){
        return leaderClientHashMap.get(node);
    }

    public Node getAnyNode(){
        return (Node) leaderClientHashMap.keySet().toArray()[0];
    }
}
