package org.ekbana.server.leader;

import java.util.HashMap;

public class NodeClientMapper {
    private final HashMap<String,LeaderClient> leaderClientHashMap=new HashMap<>();

    public void addNode(Node node,LeaderClient leaderClient){
        leaderClientHashMap.put(node.getAddress(),leaderClient);
    }

    public void removeNode(Node node){
        leaderClientHashMap.remove(node.getAddress());
    }
    public LeaderClient getLeaderClient(Node node){
        return leaderClientHashMap.get(node.getAddress());
    }

    public Node getAnyNode(){
        return new Node((String) leaderClientHashMap.keySet().toArray()[0]);
    }

    public HashMap<String,LeaderClient> getAllNodes(){
        return leaderClientHashMap;
    }
}
