package org.ekbana.broker.record;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class Record implements Serializable {
    // provided by producer client
    private String topic;
    private String data;

    // provided by the partitioner or producer-client
    private int partitionId;

    // provided by the broker
    private long offset;

    public Record(String topic, String data, int partitionId) {
        this.topic = topic;
        this.data = data;
        this.partitionId = partitionId;
    }

    public Record(String data) {
        this.data = data;
    }

    /** returns the size of record*/
    public int size(){
        return data.length();
    }
}
