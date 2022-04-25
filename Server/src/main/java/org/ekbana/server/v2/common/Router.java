package org.ekbana.server.v2.common;

import org.ekbana.server.cluster.Node;
import org.ekbana.server.common.cm.request.KafkaClientRequest;
import org.ekbana.server.common.cm.response.KafkaClientResponse;
import org.ekbana.server.common.mb.Topic;
import org.ekbana.server.common.mb.Transaction;
import org.ekbana.server.v2.client.KafkaClientController;
import org.ekbana.server.v2.leader.LeaderController;
import org.ekbana.server.v2.node.NodeController;

public interface Router {
    interface KafkaClientRouter{
        void register(KafkaClientController kafkaClientController);
        void routeToLeader(KafkaClientRequest kafkaClientRequest);
    }

    interface LeaderRouter{
        void register(LeaderController leaderController);
        void routeToClient(KafkaClientResponse kafkaClientResponse);
        void routeToNode(Transaction transaction);
        void routeToNode(KafkaClientRequest kafkaClientRequest, Topic topic);
    }

    interface NodeRouter{
        void register(NodeController nodeController);
        void routeToLeader(Transaction transaction);
        void routeToClient(KafkaClientResponse kafkaClientResponse);
    }

}
