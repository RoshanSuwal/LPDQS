package org.ekbana.server.leader;

import lombok.Getter;
import lombok.Setter;

@Getter @Setter
public class KafkaServerConfig {
    private String server_address="localhost";
    private int port=9998;

    private int queueSize=100;
}
