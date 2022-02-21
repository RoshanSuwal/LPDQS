package org.ekbana.server.leader;

import org.ekbana.server.common.mb.Topic;
import org.ekbana.server.util.Mapper;

public class TopicManager {

    private Mapper<String, Topic> topicMapper;

    public boolean valid(String topicName){
        return topicMapper.has(topicName);
    }


}
