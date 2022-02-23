package org.ekbana.server.common.cm.request;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class ConsumerRecordReadRequest extends KafkaClientRequest{
    private String topicNanme;
    private int partitionId;
    private long offset;
    private boolean isTimeOffset;

    public ConsumerRecordReadRequest(String topicName,int partitionId,long offset,boolean isTimeOffset) {
        super(RequestType.CONSUMER_RECORD_READ);
        this.topicNanme=topicName;
        this.partitionId=partitionId;
        this.offset=offset;
        this.isTimeOffset=isTimeOffset;
    }
}
