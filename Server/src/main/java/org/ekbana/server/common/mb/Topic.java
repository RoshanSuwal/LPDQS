package org.ekbana.server.common.mb;

import lombok.Builder;
import lombok.Getter;
import org.ekbana.server.leader.Node;

import java.io.Serializable;

@Getter
@Builder
public class Topic implements Serializable {
    private final String topicName;
    private final int numberOfPartitions;
    private final Node[] dataNode;
}
