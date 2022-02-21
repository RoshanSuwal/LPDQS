package org.ekbana.server.config;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;
import java.util.List;

@Getter @Setter
@NoArgsConstructor
public class TopicConfiguration implements Serializable {
    public enum TopicConfigurationType{
        PRODUCER,CONSUMER,CREATOR,DELETER
    }

    private String topicName;
    private int numberOfPartition;

    // determines the type of configuration
    private TopicConfigurationType topicConfigurationType;

    // determines the partitions for producer and consumer
    private List<Integer> partitions;

    // for consumer
    private String consumerGroup;

    // Topic Authentication
    private String username;
    private String  password;


}
