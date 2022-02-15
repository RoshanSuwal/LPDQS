package org.ekbana.broker.segment.search;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

import java.util.concurrent.atomic.AtomicBoolean;

@Getter
@ToString
@Builder
public class Node {
    private final long offset;
    private final long criteria;
//    private final long size;
    private final AtomicBoolean status=new AtomicBoolean(true);

    public Node(long offset,long criteria){
        this.offset=offset;
        this.criteria=criteria;
    }

    public void setStatus(boolean sta){
        status.set(sta);
    }

    public boolean contain(long off,boolean isCriteria){
        return status.get() && (isCriteria ? off < criteria : off < offset);
    }
}
