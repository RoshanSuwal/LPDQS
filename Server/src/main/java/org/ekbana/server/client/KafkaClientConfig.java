package org.ekbana.server.client;

import lombok.Getter;

@Getter
public class KafkaClientConfig {
    private String address="localhost";
    private int port=9999;
}
