package org.ekbana.server.common.mb;

import lombok.Builder;
import lombok.Getter;
import lombok.ToString;
import org.ekbana.server.cluster.Node;

import java.io.Serializable;

@Getter
@Builder
@ToString
public class Topic implements Serializable {
    private final String topicName;
    private final int numberOfPartitions;
    private final Node[] dataNode;
}
