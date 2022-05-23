package org.ekbana.server.config;

import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

public class KafkaProperties extends Properties {

    private final Map<String,String> defaultPropertiesValue=Map.of(
            "kafka.server.address","localhost",
            "kafka.node.server.port","9998",
            "kafka.client.server.port","9999",
//            "kafka.security.auth.username","username",
//            "kafka.security.auth.password","password",
            "kafka.server.node.id","node-0",
            "kafka.storage.data.path","log/",
            "kafka.request.queue.size","100",
            "kafka.loadbalancer.policy","weighted-round-robin",
            "kafka.server.reconnect.interval","10000",
            "kafka.plugin.dir.path","plugins"
            );

    public KafkaProperties(String filePath) throws IOException {
        final FileReader fileReader = new FileReader(filePath);
        super.load(fileReader);
    }

    public KafkaProperties(Properties properties) {
        super(properties);
    }

    public String getKafkaProperty(String key){
        return getProperty(key,defaultPropertiesValue.getOrDefault(key,""));
    }

    public String getRootPath(){
        return getKafkaProperty("kafka.storage.data.path");
    }

    public int getQueueSize() {
        return Integer.parseInt(getKafkaProperty("kafka.request.queue.size"));
    }
}
