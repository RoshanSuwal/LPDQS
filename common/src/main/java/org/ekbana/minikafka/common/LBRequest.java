package org.ekbana.minikafka.common;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

@Getter @Setter
@Builder
public class LBRequest {
    private String id;
    private String key;
    private int requestWeight;
}
