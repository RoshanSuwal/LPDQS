package org.ekbana.server.v2.common;

import org.ekbana.server.cluster.Node;
import org.ekbana.server.common.cm.request.KafkaClientRequest;
import org.ekbana.server.common.cm.response.KafkaClientResponse;
import org.ekbana.server.common.mb.Topic;
import org.ekbana.server.common.mb.Transaction;
import org.ekbana.server.v2.client.KafkaClientController;
import org.ekbana.server.v2.leader.LeaderController;
import org.ekbana.server.v2.node.NodeController;

public class KafkaRouter implements Router.KafkaClientRouter, Router.LeaderRouter, Router.NodeRouter {
    private KafkaClientController clientController;
    private LeaderController leaderController;
    private NodeController nodeController;

    @Override
    public void register(KafkaClientController kafkaClientController) {
        this.clientController = kafkaClientController;
    }

    @Override
    public void routeToLeader(KafkaClientRequest kafkaClientRequest) {
        leaderController.request(kafkaClientRequest);
    }

    @Override
    public void register(LeaderController leaderController) {
        this.leaderController = leaderController;
    }

    @Override
    public void routeToClient(KafkaClientResponse kafkaClientResponse) {
        this.clientController.response(kafkaClientResponse);
    }

    @Override
    public void routeToNode(Transaction transaction) {
        System.out.println(transaction);
    }

    @Override
    public void routeToNode(KafkaClientRequest kafkaClientRequest, Topic topic) {
        nodeController.processRequest(kafkaClientRequest,topic);
    }

    @Override
    public void register(NodeController nodeController) {
        this.nodeController = nodeController;
    }

    @Override
    public void routeToLeader(Transaction transaction) {

    }
}
