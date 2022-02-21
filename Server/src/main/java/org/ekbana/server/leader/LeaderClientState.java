package org.ekbana.server.leader;

public enum LeaderClientState {
    NEW,
    CONNECTING,CONNECTED,
    AUTHENTICATING,AUTHENTICATED,
    CLOSING,CLOSE,CLOSED
}
