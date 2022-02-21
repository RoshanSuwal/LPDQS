package org.ekbana.server.cluster;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

@Getter
@Setter
@AllArgsConstructor
public class Node implements Serializable {
    private String address;

    public boolean equals(Node node) {
        return node.getAddress().equals(address);
    }
}
