package org.ekbana.server.common.lr;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

import java.io.Serializable;

@AllArgsConstructor
@Getter
@ToString
public class RTransaction implements Serializable {

    public enum RRequestType{
        CREATE_TOPIC,
        DELETE_TOPIC,
        OFFSET_COMMIT
    }

    public enum RTransactionType{
        REGISTER,ACKNOWLEDGE,COMMIT,
        ABORT,SYNC
    }

    private final long rTransactionId;
    private final RTransactionType rTransactionType;

}
