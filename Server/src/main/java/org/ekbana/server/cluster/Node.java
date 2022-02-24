package org.ekbana.server.cluster;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

import java.io.Serializable;
import java.util.Objects;

@Getter
@AllArgsConstructor
@ToString
public class Node implements Serializable {
    private final String id;
    private final String address;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Node node = (Node) o;
        return Objects.equals(address, node.address);
    }

    @Override
    public int hashCode() {
        return Objects.hash(address);
    }
}
