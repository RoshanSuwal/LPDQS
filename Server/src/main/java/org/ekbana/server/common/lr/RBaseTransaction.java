package org.ekbana.server.common.lr;

import lombok.Getter;
import org.ekbana.server.common.mb.Topic;

import java.io.Serializable;

@Getter
public class RBaseTransaction extends RTransaction implements Serializable {

    private final Topic topic;
    private final RRequestType rRequestType;

    public RBaseTransaction(long rTransactionId,RTransactionType rTransactionType,RRequestType rRequestType,Topic topic){
        super(rTransactionId,rTransactionType);
        this.topic=topic;
        this.rRequestType=rRequestType;
    }

}
