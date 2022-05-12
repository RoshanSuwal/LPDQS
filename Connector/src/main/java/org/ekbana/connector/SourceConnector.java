package org.ekbana.connector;

import com.google.gson.Gson;
import org.apache.commons.io.FileUtils;
import org.ekbana.minikafka.client.producer.KafkaProducer;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class SourceConnector implements KafkaProducer.ProducerEventListener {

    private static final Logger logger = LoggerFactory.getLogger(SourceConnector.class);
    private ConnectorConfigurationProperty connectorConfigurationProperty;
    private JDBCConnector jdbcConnector;
    private KafkaProducer kafkaProducer;
    private AtomicBoolean readyToFetchNextBatch;
    private long count = 0;
    private long lastFetchTimeStamp = 0L;

    public SourceConnector(ConnectorConfigurationProperty connectorConfigurationProperty) {
        this.connectorConfigurationProperty = connectorConfigurationProperty;
        this.jdbcConnector = new JDBCConnector(connectorConfigurationProperty);
        this.kafkaProducer = new KafkaProducer(kafkaPropertiesFromConnectorProperty(connectorConfigurationProperty), this);
        readyToFetchNextBatch = new AtomicBoolean(false);
    }

    public void connect() {
        try {
            this.jdbcConnector.connect();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

        try {
            this.kafkaProducer.connect();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        startPolling();
    }

    private void startPolling() {
        if (connectorConfigurationProperty.getIncrementalColumnName()!=null) {
            connectorConfigurationProperty.setIncrementalColumnValue(connectorConfigurationProperty.getPrevIncrementalColumnValue());
        }

        if(connectorConfigurationProperty.getIncrementalTimeStampColumnName()!=null){
            connectorConfigurationProperty.setPrevIncrementalTimeStampColumnValue(connectorConfigurationProperty.getPrevIncrementalTimeStampColumnValue());
        }

        while (true) {
            if (readyToFetchNextBatch.get()) {
                while (Instant.now().getEpochSecond() - lastFetchTimeStamp < connectorConfigurationProperty.getPoolingInterval()) ;
                fetchRecordsFromJDBC();
                lastFetchTimeStamp = Instant.now().getEpochSecond();
            }
            try {
                Thread.sleep(5);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void fetchRecordsFromJDBC() {
        logger.info("Fetching records from JDBC source");
        final List<JSONObject> jsonObjects = this.jdbcConnector.executeSelectOperation();
        count = count + jsonObjects.size();
        logger.info("Fetch records count : {}  Total Count : {}", jsonObjects.size(),count);
        if (jsonObjects.size() > 0) {
            jsonObjects.stream()
//                    .peek(jsonObject -> logger.info(jsonObject.toString()))
                    .forEach(jsonObject -> kafkaProducer.send(jsonObject.toString()));
            readyToFetchNextBatch.set(false);
        }
    }

    private Properties kafkaPropertiesFromConnectorProperty(ConnectorConfigurationProperty connectorConfigurationProperty) {
        Properties properties = new Properties();
        properties.setProperty("kafka.server.address", connectorConfigurationProperty.getKafkaServerAddress());
        properties.setProperty("kafka.server.port", connectorConfigurationProperty.getKafkaServerPort());
        properties.setProperty("kafka.topic.name", connectorConfigurationProperty.getTopicName());
        return properties;
    }

    public void saveToFile(){
        try {
            FileUtils.writeByteArrayToFile(new File(ConnectorLauncher.PATH+"/source/"+connectorConfigurationProperty.getConnectorName()+".json"),new Gson().toJson(connectorConfigurationProperty).getBytes());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void onProducerRecordWriteCompleted() {
        // triggers the polling
        logger.info("All records send complete, fetching next batch of records");
        saveToFile();
        readyToFetchNextBatch.set(true);
    }

    @Override
    public void onProducerConnectionClose() {
        // handle the re-connect attempts
        System.exit(0);
    }

    @Override
    public void onProducerConfigurationSuccess() {
        logger.info("Producer Configured Successfully.");
        readyToFetchNextBatch.set(true);
    }
}
