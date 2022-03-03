package org.ekbana.broker.consumer;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import org.ekbana.broker.record.Recorder;
import org.ekbana.minikafka.common.ConsumerRecords;

@Getter @Setter @AllArgsConstructor
public class Consumer {
    private Recorder recorder;
    private String topicName;

    public ConsumerRecords getRecords(long offset, boolean isTimeOffset){
        return recorder.getRecords(offset,isTimeOffset);
    }
}
