package org.ekbana.server.leader;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.io.Serializable;

@Getter
@AllArgsConstructor
public class Node implements Serializable {
    private final String address;
}
