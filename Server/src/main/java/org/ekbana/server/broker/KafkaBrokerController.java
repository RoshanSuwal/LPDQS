package org.ekbana.server.broker;

import lombok.AllArgsConstructor;
import org.ekbana.broker.Broker;
import org.ekbana.server.common.Router;
import org.ekbana.server.common.mb.*;
import org.ekbana.server.util.Mapper;

import java.util.concurrent.ExecutorService;

@AllArgsConstructor
public class KafkaBrokerController {
    private final Broker broker;
    private final ExecutorService executorService;
    private final Router.KafkaBrokerRouter kafkaBrokerRouter;

    private final Mapper<Long,RequestTransaction> brokerTransactionMapper;

    public void request(Transaction transaction){
        if (transaction.getAction()== TransactionType.Action.REGISTER) {
            brokerTransactionMapper.add(transaction.getTransactionId(), (RequestTransaction) transaction);
            response(new BaseResponseTransaction(transaction.getTransactionId(), TransactionType.Action.ACKNOWLEDGE));
        }else if (transaction.getAction()==TransactionType.Action.COMMIT){
            // execute the transaction according to action
            executorService.execute(()->processTransaction(brokerTransactionMapper.get(transaction.getTransactionId())));
        }else if (transaction.getAction()==TransactionType.Action.ABORT){
            brokerTransactionMapper.delete(transaction.getTransactionId());
        }
    }

    public void response(Transaction transaction){
        kafkaBrokerRouter.routeFromBrokerToFollower(transaction);
    }

    private void processTransaction(RequestTransaction requestTransaction){
        // 4 types of transaction
        // 1. create topic partition -- asynchronous
        // 2. delete topic partition -- asynchronous
        // 3. write producer record into topic partition -- synchronous -> asynchronous operation
        // 4. read consumer record from topic partition  -- asynchronous
        switch (requestTransaction.getRequestType()){
            case TOPIC_PARTITION_CREATE ->{
                final TopicCreateRequestTransaction topicCreateRequestTransaction = (TopicCreateRequestTransaction) requestTransaction;
                for (int partition : topicCreateRequestTransaction.getPartitions()) {
                    broker.createTopic(topicCreateRequestTransaction.getTopic().getTopicName(),partition);
                }
            }case TOPIC_PARTITION_DELETE -> {
                final TopicDeleteRequestTransaction topicDeleteRequestTransaction = (TopicDeleteRequestTransaction) requestTransaction;
                for (int partition : topicDeleteRequestTransaction.getPartitions()) {
                    broker.createTopic(topicDeleteRequestTransaction.getTopic().getTopicName(),partition);
                }
            }case CONSUMER_RECORD_READ -> {
                final ConsumerRecordReadRequestTransaction consumerRecordReadRequestTransaction = (ConsumerRecordReadRequestTransaction) requestTransaction;
                broker.getConsumer(consumerRecordReadRequestTransaction.getTopic().getTopicName(),consumerRecordReadRequestTransaction.getPartition())
                        .getRecords(consumerRecordReadRequestTransaction.getOffset(),consumerRecordReadRequestTransaction.isTimeOffset());
            }case PRODUCER_RECORD_WRITE -> {
                final ProducerRecordWriteRequestTransaction producerRecordWriteRequestTransaction=(ProducerRecordWriteRequestTransaction)requestTransaction;
                broker.getProducer(producerRecordWriteRequestTransaction.getTopic().getTopicName(),producerRecordWriteRequestTransaction.getPartition())
                        .addRecords(null);
            }
        }
    }
}
