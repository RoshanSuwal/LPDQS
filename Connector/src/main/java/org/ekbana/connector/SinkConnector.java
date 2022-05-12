package org.ekbana.connector;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.apache.commons.io.FileUtils;
import org.ekbana.minikafka.client.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Properties;

public class SinkConnector {
    private static final Logger logger=LoggerFactory.getLogger(SinkConnector.class);
    private ConnectorConfigurationProperty connectorConfigurationProperty;
    private JDBCConnector jdbcConnector;
    private KafkaConsumer kafkaConsumer;

    public SinkConnector(ConnectorConfigurationProperty connectorConfigurationProperty) {
        this.connectorConfigurationProperty = connectorConfigurationProperty;
        this.jdbcConnector=new JDBCConnector(connectorConfigurationProperty);
        this.kafkaConsumer=new KafkaConsumer(kafkaPropertiesFromConnectorProperty(connectorConfigurationProperty));
    }

    public void connect(){
        try {
            this.jdbcConnector.connect();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

        try {
            this.kafkaConsumer.connect();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        startPolling();
    }

    public void startPolling(){
        try {
            final JsonObject records = this.kafkaConsumer.getRecords();
            if (records.get("endingOffset").getAsLong()!=-1){
                connectorConfigurationProperty.getLastFetchedTopicOffset()
                        .put(records.get("partitionId").getAsString(),records.get("endingOffset").getAsLong());
            }

            final JsonArray records1 = records.getAsJsonArray("records");
            dumpRecordsToJDBC(records1);
            saveToFile();

        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public void dumpRecordsToJDBC(JsonArray records){
        if (records.size()>0){
            jdbcConnector.executeBatchInsertOperation(records);
        }
    }

    public void saveToFile(){
        try {
            FileUtils.writeByteArrayToFile(new File(ConnectorLauncher.PATH+"/sink/"+connectorConfigurationProperty.getConnectorName()+".json"),new Gson().toJson(connectorConfigurationProperty).getBytes());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Properties kafkaPropertiesFromConnectorProperty(ConnectorConfigurationProperty connectorConfigurationProperty){
        Properties properties=new Properties();
        properties.setProperty("kafka.server.address", connectorConfigurationProperty.getKafkaServerAddress());
        properties.setProperty("kafka.server.port", connectorConfigurationProperty.getKafkaServerPort());
        properties.setProperty("kafka.topic.name", connectorConfigurationProperty.getTopicName());
        return properties;
    }
}
