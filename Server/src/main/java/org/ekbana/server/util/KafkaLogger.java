package org.ekbana.server.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaLogger {

    public static Logger kafkaLogger=LoggerFactory.getLogger("Kafka");
    public static Logger clientLogger= LoggerFactory.getLogger("Client");
    public static Logger leaderLogger=LoggerFactory.getLogger("Leader");
    public static Logger nodeLogger=LoggerFactory.getLogger("Node");
    public static Logger transactionLogger=LoggerFactory.getLogger("Transaction");
    public static Logger topicLogger=LoggerFactory.getLogger("Topic");

    public static Logger networkLogger=LoggerFactory.getLogger("Network");

    public static Logger dataNodeLogger=LoggerFactory.getLogger("DataNode");

}
