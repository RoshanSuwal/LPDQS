package org.ekbana.server.replica;

import org.ekbana.server.common.mb.ConsumerGroup;
import org.ekbana.server.common.mb.Topic;


public interface Replica {
    Topic getTopic(String topicName);
    boolean hasTopic(String topicName);

    ConsumerGroup getConsumerGroup(String topic, String groupName);
    boolean exists(String topic,String groupName);
}
