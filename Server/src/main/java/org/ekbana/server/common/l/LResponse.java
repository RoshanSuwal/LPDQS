package org.ekbana.server.common.l;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

@Getter @Setter
@AllArgsConstructor
public class LResponse implements Serializable {
    private FollowerMode mode;
    private Object object;
}
