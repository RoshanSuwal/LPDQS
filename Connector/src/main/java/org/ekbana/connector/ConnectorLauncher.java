package org.ekbana.connector;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.FileReader;

public class ConnectorLauncher {

//    public static String PATH="/home/roshan/workspace/ekbana/study/MiniKafkaSystem/connector-config/";
    public static String PATH;
    private static final Logger logger = LoggerFactory.getLogger(ConnectorLauncher.class);

//    public static void sourceConnector(ConnectorConfigurationProperty connectorConfigurationProperty){
//        final SourceConnector sourceConnector = new SourceConnector(connectorConfigurationProperty);
//        sourceConnector.connect();
//    }
//
//    public static void sinkConnector(ConnectorConfigurationProperty connectorConfigurationProperty){
//        final SourceConnector sourceConnector = new SourceConnector(connectorConfigurationProperty);
//        sourceConnector.connect();
//    }
//
//    public static void sourceConnector1(){
//        final JSONObject connectorConfig = new JSONObject()
//                .put("connectorType", "source")
//                .put("connectorName", "test sink connector")
//                .put("kafkaServerAddress", "localhost")
//                .put("kafkaServerPort", "9999")
//                .put("topicName", "test")
//                .put("jdbcDriver", "org.postgresql.Driver")
//                .put("jdbcUrl", "jdbc:postgresql://localhost:5432/rad")
//                .put("jdbcUsername", "postgres")
//                .put("jdbcPassword", "root")
//                .put("table", "tweet1")
//                .put("incrementalColumnName", "id")
////                .put("incrementalTimeStampColumnName","")
//                .put("fetchSize", 20)
//                .put("poolingInterval", 5);
//
//        final ConnectorConfigurationProperty connectorConfigurationProperty = new Gson().fromJson(connectorConfig.toString(), ConnectorConfigurationProperty.class);
//        final SourceConnector sourceConnector = new SourceConnector(connectorConfigurationProperty);
//        sourceConnector.connect();
//    }
//
//    public static void sinkConnector(){
//        final JSONObject sinkConfig = new JSONObject()
//                .put("connectorType", "source")
//                .put("connectorName", "test source connector")
//                .put("kafkaServerAddress", "localhost")
//                .put("kafkaServerPort", "9999")
//                .put("topicName", "test")
//                .put("jdbcDriver", "org.postgresql.Driver")
//                .put("jdbcUrl", "jdbc:postgresql://localhost:5432/rad")
//                .put("jdbcUsername", "postgres")
//                .put("jdbcPassword", "root")
//                .put("table", "tweet1")
////                .put("incrementalColumnName", "id")
////                .put("incrementalTimeStampColumnName","")
//                .put("batchSize", 20)
//                .put("poolingInterval", 5);
//
//        final ConnectorConfigurationProperty connectorConfigurationProperty = new Gson().fromJson(sinkConfig.toString(), ConnectorConfigurationProperty.class);
//        final SinkConnector sinkConnector = new SinkConnector(connectorConfigurationProperty);
//        sinkConnector.connect();
//    }

    public static void main(String[] args) throws FileNotFoundException {
        PATH=System.getProperty("path");

        if (PATH==null){
            logger.info("please specify connector-config-path");
            System.exit(0);
        }

        final String connectorPath = System.getProperty("config");
        if (connectorPath==null) {
            logger.info("please specify connector property");
            System.exit(0);
        }

        JsonParser jsonParser=new JsonParser();
        final JsonObject connectorProperty = jsonParser.parse(new FileReader(connectorPath)).getAsJsonObject();
        final ConnectorConfigurationProperty connectorConfigurationProperty = new Gson().fromJson(connectorProperty.toString(), ConnectorConfigurationProperty.class);
        switch (connectorConfigurationProperty.getConnectorType()){
            case "source"->{
                final SourceConnector sourceConnector = new SourceConnector(connectorConfigurationProperty);
                Runtime.getRuntime().addShutdownHook(new Thread(()->{
                    logger.info("Closing the Application");
                    sourceConnector.saveToFile();
                }));
                sourceConnector.connect();
            }case "sink"->{
                final SinkConnector sinkConnector = new SinkConnector(connectorConfigurationProperty);
                Runtime.getRuntime().addShutdownHook(new Thread(()->{
                    logger.info("Closing the Application");
                    sinkConnector.saveToFile();
                }));
                sinkConnector.connect();
            }default -> {
                logger.info("Invalid connector type : {}",connectorConfigurationProperty.getConnectorType());
            }
        }

    }

    // contain properties file already
//    public static void main1(String[] args) throws IOException, SQLException, ClassNotFoundException {
//        final JSONObject connectorConfig = new JSONObject()
//                .put("connectorType", "source")
//                .put("connectorName", "test source connector")
//                .put("kafkaServerAddress", "localhost")
//                .put("kafkaServerPort", "9999")
//                .put("topicName", "test")
//                .put("jdbcDriver", "org.postgresql.Driver")
//                .put("jdbcUrl", "jdbc:postgresql://localhost:5432/rad")
//                .put("jdbcUsername", "postgres")
//                .put("jdbcPassword", "root")
//                .put("table", "tweet1")
//                .put("incrementalColumnName", "id")
////                .put("incrementalTimeStampColumnName","")
//                .put("fetchSize", 10)
//                .put("poolingTime", 5);
//
//        final JSONObject sinkConfig = new JSONObject()
//                .put("connectorType", "source")
//                .put("connectorName", "test source connector")
//                .put("kafkaServerAddress", "localhost")
//                .put("kafkaServerPort", "9999")
//                .put("topicName", "test")
//                .put("jdbcDriver", "org.postgresql.Driver")
//                .put("jdbcUrl", "jdbc:postgresql://localhost:5432/rad")
//                .put("jdbcUsername", "postgres")
//                .put("jdbcPassword", "root")
//                .put("table", "tweet1")
//                .put("incrementalColumnName", "id")
////                .put("incrementalTimeStampColumnName","")
//                .put("fetchSize", 10)
//                .put("poolingTime", 5);
//
//
//        final ConnectorConfigurationProperty connectorConfigurationProperty = new Gson().fromJson(connectorConfig.toString(), ConnectorConfigurationProperty.class);
//        final ConnectorConfigurationProperty sinkConnectorConfigurationProperty = new Gson().fromJson(sinkConfig.toString(), ConnectorConfigurationProperty.class);
//        JDBCConnector jdbcConnector = new JDBCConnector(connectorConfigurationProperty);
//        JDBCConnector sinkJdbcConnector = new JDBCConnector(sinkConnectorConfigurationProperty);
//        jdbcConnector.connect();
//        sinkJdbcConnector.connect();
//
//        KafkaProducer.ProducerEventListener producerEventListener=new KafkaProducer.ProducerEventListener() {
//            @Override
//            public void onProducerRecordWriteCompleted() {
//
//            }
//
//            @Override
//            public void onProducerConnectionClose() {
//
//            }
//
//            @Override
//            public void onProducerConfigurationSuccess() {
//
//            }
//        };
//
//        KafkaProducer kafkaProducer = new KafkaProducer(kafkaPropertiesFromConnectorProperty(connectorConfigurationProperty),producerEventListener);
//
//        System.out.println(connectorConfigurationProperty);
//
//        for (int i = 0; i < 60; i++) {
//            final List<JSONObject> jsonObjects = jdbcConnector.executeSelectOperation();
//            for (JSONObject jsonObject : jsonObjects) {
//                logger.info(jsonObject.toString());
//            }
//            sinkJdbcConnector.executeBatchInsertOperation(jsonObjects);
//        }
////        jdbcConnector.connect();
////        kafkaProducer.connect();
//
//    }
//
//    public static JSONObject jdbc() {
//        return new JSONObject()
//                .put("jdbcDriver", "org.postgresql.Driver")
//                .put("jdbcUrl", "jdbc:postgresql://localhost:5432/rad")
//                .put("jdbcUsername", "postgres")
//                .put("jdbcPassword", "password")
//                .put("table", "tweet")
//                .put("primaryKeyColumnName", "id")
//                .put("incrementalColumnName", "id")
////                .put("incrementalTimeStampColumnName","")
//                .put("fetchSize", 1)
//                .put("poolingTime", 5);
//    }
//
//    public static JSONObject miniKafka() {
//        return new JSONObject()
//                .put("kafkaServerAddress", "localhost")
//                .put("kafkaServerPort", "9999")
//                .put("topicName", "test");
//    }
//
//    public static Properties jsonToProperties(JSONObject jsonObject) {
//        Properties properties = new Properties();
//        properties.setProperty("kafka.server.address", jsonObject.getString("server"));
//        properties.setProperty("kafka.server.port", jsonObject.getString("port"));
//        properties.setProperty("kafka.topic.name", jsonObject.getString("topic"));
//        return properties;
//    }
//
//    public static Properties kafkaPropertiesFromConnectorProperty(ConnectorConfigurationProperty connectorConfigurationProperty) {
//        Properties properties = new Properties();
//        properties.setProperty("kafka.server.address", connectorConfigurationProperty.getKafkaServerAddress());
//        properties.setProperty("kafka.server.port", connectorConfigurationProperty.getKafkaServerPort());
//        properties.setProperty("kafka.topic.name", connectorConfigurationProperty.getTopicName());
//        return properties;
//    }
}
