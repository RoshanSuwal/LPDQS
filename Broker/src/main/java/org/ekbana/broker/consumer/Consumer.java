package org.ekbana.broker.consumer;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import org.ekbana.broker.record.Recorder;
import org.ekbana.broker.record.Records;

@Getter @Setter @AllArgsConstructor
public class Consumer {
    private Recorder recorder;
    private String topicName;

    public Records getRecords(long offset,boolean isTimeOffset){
        return recorder.getRecords(offset,isTimeOffset);
    }
}
