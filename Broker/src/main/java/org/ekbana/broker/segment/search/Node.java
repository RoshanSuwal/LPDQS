package org.ekbana.broker.segment.search;

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
    private AtomicBoolean status;

    public Node(long offset,long criteria,AtomicBoolean status){
        this.offset=offset;
        this.criteria=criteria;
        this.status=status;
    }

    public void setStatus(boolean sta){
        status.set(sta);
    }

    public boolean contain(long off,boolean isCriteria){
        return status.get() && (isCriteria ? off < criteria : off < offset);
    }
}
