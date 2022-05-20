package org.ekbana.broker.utils;

import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

public class KafkaBrokerProperties extends Properties {

    private final Map<String,String> defaultBrokerPropertiesValue=Map.of(
            "kafka.broker.segment.batch.policy","offset-count-based-segment-batch-policy",
            "kafka.broker.consumer.record.batch.policy","offset-count-based-consumer-records-batch-policy",
            "kafka.broker.segment.retention.policy","time-based-segment-retention-policy",
            "kafka.storage.data.path","log/",
            "kafka.broker.data.path","data/",
            "kafka.broker.segment.search.tree.node.capacity","3",
            "kafka.server.reconnect.interval","10000",
            "kafka.broker.schedule.interval","60"
    );

    public KafkaBrokerProperties(String filePath) throws IOException {
        final FileReader fileReader = new FileReader(filePath);
        super.load(fileReader);
    }

    public KafkaBrokerProperties(Properties properties) {
        super(properties);
    }

    public String getBrokerProperty(String key){
        return getProperty(key,defaultBrokerPropertiesValue.getOrDefault(key,""));
    }

    public String getDataPath(){
//        return getRootPath()+"data/";
        return getBrokerProperty("kafka.broker.data.path");
    }

    public String getRootPath(){
        return getBrokerProperty("kafka.storage.data.path");
    }

    public String getSegmentFileName() {
        return "segment.txt";
    }

    public String getTopicMataDataFileName() {
        return "topicMetaData.txt";
    }
}
