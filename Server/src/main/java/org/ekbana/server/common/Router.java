package org.ekbana.server.common;

import org.ekbana.server.broker.KafkaBrokerController;
import org.ekbana.server.client.KafkaClientController;
import org.ekbana.server.common.cm.request.KafkaClientRequest;
import org.ekbana.server.common.cm.response.KafkaClientResponse;
import org.ekbana.server.common.lr.RTransaction;
import org.ekbana.server.common.mb.Transaction;
import org.ekbana.server.follower.FollowerController;
import org.ekbana.server.replica.ReplicaController;

public interface Router {
    public interface KafkaClientRouter{
        void register(KafkaClientController kafkaClientController);
        void routeFromClientToFollower(KafkaClientRequest kafkaClientRequest);
        void routeFromClientToLeader(KafkaClientRequest kafkaClientRequest);
    }

    public interface KafkaBrokerRouter{
        void register(KafkaBrokerController kafkaBrokerController);
        void routeFromBrokerToFollower(Transaction transaction);
    }

    public interface KafkaFollowerRouter{
        void register(FollowerController followerController);
        void routeFromFollowerToClient(KafkaClientResponse kafkaClientResponse);
        void routeFromFollowerToBroker(Transaction transaction);
        void routeFromFollowerToReplica(RTransaction rTransaction);
    }

    interface KafkaReplicaRouter{
        void register(ReplicaController replicaController);
        void routeFromReplicaToFollower(RTransaction rTransaction);
    }
}
