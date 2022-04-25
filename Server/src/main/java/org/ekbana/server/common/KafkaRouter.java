package org.ekbana.server.common;

import org.ekbana.server.broker.KafkaBrokerController;
import org.ekbana.server.client.KafkaClientController;
import org.ekbana.server.common.cm.request.KafkaClientRequest;
import org.ekbana.server.common.cm.response.KafkaClientResponse;
import org.ekbana.server.common.lr.RTransaction;
import org.ekbana.server.common.mb.Transaction;
import org.ekbana.server.follower.FollowerController;
import org.ekbana.server.replica.ReplicaController;

public class KafkaRouter implements Router.KafkaFollowerRouter, Router.KafkaBrokerRouter, Router.KafkaClientRouter,Router.KafkaReplicaRouter {

    private KafkaClientController kafkaClientController;
    private KafkaBrokerController kafkaBrokerController;
    private FollowerController followerController;
    private ReplicaController replicaController;

    @Override
    public void register(KafkaClientController kafkaClientController) {
        this.kafkaClientController=kafkaClientController;
    }

    @Override
    public void routeFromClientToFollower(KafkaClientRequest kafkaClientRequest) {
        if (followerController!=null)
            followerController.registerRequest(kafkaClientRequest);
    }

    @Override
    public void routeFromClientToLeader(KafkaClientRequest kafkaClientRequest) {
    }

    @Override
    public void register(KafkaBrokerController kafkaBrokerController) {
        this.kafkaBrokerController=kafkaBrokerController;
    }

    @Override
    public void routeFromBrokerToFollower(Transaction transaction) {
        if (followerController!=null)
            followerController.registerTransaction(transaction);
    }

    @Override
    public void register(FollowerController followerController) {
        this.followerController=followerController;
    }

    @Override
    public void routeFromFollowerToClient(KafkaClientResponse kafkaClientResponse) {
        if (kafkaClientController!=null)
            kafkaClientController.response(kafkaClientResponse);
    }

    @Override
    public void routeFromFollowerToBroker(Transaction transaction) {
        if (kafkaBrokerController!=null)
            kafkaBrokerController.request(transaction);
    }

    @Override
    public void register(ReplicaController replicaController) {
        this.replicaController=replicaController;
    }

    @Override
    public void routeFromReplicaToFollower(RTransaction rTransaction) {
        if (followerController!=null)
            followerController.registerRTransaction(rTransaction);
    }

    @Override
    public void routeFromFollowerToReplica(RTransaction rTransaction) {
        if (replicaController!=null){
            replicaController.fromLeader(rTransaction);
        }
    }

}
