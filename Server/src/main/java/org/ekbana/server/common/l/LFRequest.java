package org.ekbana.server.common.l;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

import java.io.Serializable;

@Getter
@AllArgsConstructor
@ToString
public class LFRequest implements Serializable {
    public enum LFRequestType{
        AUTH,NEW
    }
    private  String nodeId;
    private  String username;
    private  String password;
    private final LFRequestType lfRequestType;

    public LFRequest(LFRequestType lfRequestType){
        this.lfRequestType=lfRequestType;
    }
}
