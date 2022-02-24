package org.ekbana.server.replica;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.ekbana.broker.utils.FileUtil;
import org.ekbana.server.common.Router;
import org.ekbana.server.common.lr.RBaseTransaction;
import org.ekbana.server.common.lr.RTransaction;
import org.ekbana.server.common.mb.ConsumerGroup;
import org.ekbana.server.common.mb.Topic;
import org.ekbana.server.leader.KafkaServerConfig;
import org.ekbana.server.util.Mapper;

import java.io.IOException;

@Getter
@AllArgsConstructor
public class ReplicaController implements Replica {
    // manages topics
    // manages consumers
    private final KafkaServerConfig kafkaServerConfig;
    private final Mapper<String, Topic> topicMapper;
    private final Mapper<String ,Object> consumerGroupMapper;
    private final Mapper<Long,RBaseTransaction> rTransactionMapper;
    private final Router.KafkaReplicaRouter kafkaReplicaRouter;

    public void log(String fromTo,Object obj){
        System.out.println("[replica] ["+fromTo+"] "+obj);
    }
    public void fromLeader(RTransaction rTransaction){
        log("follower",rTransaction);
        if (rTransaction.getRTransactionType()== RTransaction.RTransactionType.SYNC){
            sync(rTransaction);
        }else if (rTransaction.getRTransactionType()== RTransaction.RTransactionType.REGISTER){
            rTransactionMapper.add(rTransaction.getRTransactionId(), (RBaseTransaction) rTransaction);
            toLeader(new RTransaction(rTransaction.getRTransactionId(), RTransaction.RTransactionType.ACKNOWLEDGE));
        }else if (rTransaction.getRTransactionType()== RTransaction.RTransactionType.COMMIT){
            if (rTransactionMapper.has(rTransaction.getRTransactionId()))
                process(rTransactionMapper.get(rTransaction.getRTransactionId()));

        }else if (rTransaction.getRTransactionType()== RTransaction.RTransactionType.ABORT){
            rTransactionMapper.delete(rTransaction.getRTransactionId());
        }
    }

    private void sync(RTransaction rTransaction){
        // check if sync
        // if not send sync request in case for data
        process((RBaseTransaction) rTransaction);
    }

    private void process(RBaseTransaction rBaseTransaction){
        if (rBaseTransaction.getRRequestType()== RTransaction.RRequestType.CREATE_TOPIC){
            addTopic(rBaseTransaction.getTopic());
        }else if (rBaseTransaction.getRRequestType()== RTransaction.RRequestType.DELETE_TOPIC){
            deleteTopic(rBaseTransaction.getTopic());
        }
        rTransactionMapper.delete(rBaseTransaction.getRTransactionId());
    }

    public void toLeader(RTransaction rTransaction){
        kafkaReplicaRouter.routeFromReplicaToFollower(rTransaction);
    }

    public void addTopic(Topic topic){
        topicMapper.add(topic.getTopicName(),topic);
        save();
    }

    public void deleteTopic(Topic topic){
        topicMapper.delete(topic.getTopicName());
        FileUtil.deleteFile(kafkaServerConfig.getDataPath()+"topic/"+topic.getTopicName()+".txt");
    }

    public void load(){
        loadTopic();
        loadConsumerGroup();
    }

    public void save(){
        topicMapper.forEach((key,value)->{
            System.out.println(value.getTopicName());
            try {
                FileUtil.writeObjectToFile(kafkaServerConfig.getDataPath()+"topic/"+key+".txt",value);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        consumerGroupMapper.forEach((key,value)->{
            System.out.println(value);
            try {
                FileUtil.writeObjectToFile(kafkaServerConfig.getDataPath()+"consumer/"+key+".txt",value);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    private void loadTopic(){
        try{
            FileUtil.getFiles(kafkaServerConfig.getDataPath()+"topic/")
                    .peek(file -> System.out.println(file.getPath()))
                    .forEach(file -> {
                        try {
                            final Topic topic= (Topic) FileUtil.readObjectFromFile(file.getPath());
                            topicMapper.add(topic.getTopicName(),topic);
                        } catch (IOException | ClassNotFoundException e) {
                            e.printStackTrace();
                        }
                    });

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void loadConsumerGroup(){
        try{
            FileUtil.getFiles(kafkaServerConfig.getDataPath()+"consumer/")
                    .forEach(file -> {
                        try {
                            final Object o = FileUtil.readObjectFromFile(file.getPath());
//                            consumerGroupMapper.add(topic.getTopicName(),topic);
                        } catch (IOException | ClassNotFoundException e) {
                            e.printStackTrace();
                        }
                    });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Topic getTopic(String topicName) {
        return topicMapper.get(topicName);
    }

    @Override
    public boolean hasTopic(String topicName) {
        return topicMapper.has(topicName);
    }

    @Override

    public ConsumerGroup getConsumerGroup(String topic, String groupName) {
        return null;
    }

    @Override
    public boolean exists(String topic, String groupName) {
        return false;
    }
}
