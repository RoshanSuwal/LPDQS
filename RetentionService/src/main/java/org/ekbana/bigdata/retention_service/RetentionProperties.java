package org.ekbana.bigdata.retention_service;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

public class RetentionProperties extends Properties {

    private final Map<String, String> defaultPropertiesMap = Map.of(
            "kafka.retention.schedule.period", "300",
            "kafka.storage.data.path", "log/",
            "kafka.topic.segment.filename", "segment.txt",
            "kafka.broker.data.path", "data/"
    );

    public RetentionProperties(String filePath) throws IOException {
        final FileReader fileReader = new FileReader(filePath);
        super.load(fileReader);
    }

    public RetentionProperties(Properties properties) {
        super(properties);
    }

    public String getRetentionProperty(String key) {
        return getProperty(key, defaultPropertiesMap.getOrDefault(key, ""));
    }
}
