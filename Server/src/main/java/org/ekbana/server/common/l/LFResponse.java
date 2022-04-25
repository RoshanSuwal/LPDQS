package org.ekbana.server.common.l;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

import java.io.Serializable;

@AllArgsConstructor
@Getter
@ToString
public class LFResponse implements Serializable {
     public enum LFResponseType{
         CONNECTED,AUTHENTICATED,UNAUTHENTICATED,CONFIGURED
    }

    private final LFResponseType lfResponseType;
}
