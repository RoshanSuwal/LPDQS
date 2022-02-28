package org.ekbana.minikafka.common;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class ConsumerRecords {
    private long size;
    private int recordsCount;
}
