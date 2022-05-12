package org.ekbana.connector;

import lombok.*;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

@Getter @Setter
@AllArgsConstructor @NoArgsConstructor
@ToString
public class ConnectorConfigurationProperty implements Serializable {

    private String connectorName;
    private String connectorType;

    // jdbc
    private String jdbcDriver;
    private String jdbcUrl;
    private String jdbcUsername;
    private String jdbcPassword;

    private String table;
    private String query;
    private String primaryKeyColumnName;

    private String incrementalColumnName;
    private long incrementalColumnValue=0;

    private String incrementalTimeStampColumnName;
    private long incrementalTimeStampColumnValue=0;

    private int fetchSize=1;
    private int batchSize=10;

    private long poolingInterval=5;// in sec

    // kafka producer
    private String kafkaServerAddress="localhost";
    private String kafkaServerPort="9999";
    private String kafkaAuthUsername="user";
    private String kafkaAuthPassword="password";

    private String topicName;
    private String topicPartitionId="-1";
    private String topicKey;

    private long prevIncrementalColumnValue=0;
    private long prevIncrementalTimeStampColumnValue=0;

    private Map<String,Long> lastFetchedTopicOffset=new HashMap<>();
}
