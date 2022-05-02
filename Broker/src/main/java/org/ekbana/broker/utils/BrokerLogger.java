package org.ekbana.broker.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BrokerLogger {
    public static Logger searchTreeLogger= LoggerFactory.getLogger("Search Tree");
    public static Logger producerLogger = LoggerFactory.getLogger("Producer");
    public static Logger consumerLogger = LoggerFactory.getLogger("consumer");
    public static Logger brokerLogger=LoggerFactory.getLogger("Broker");
}
