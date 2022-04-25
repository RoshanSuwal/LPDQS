package org.ekbana.server.leader;

public enum LeaderClientState {
    NEW,
    CONNECTING,CONNECTED,
    AUTHENTICATING,AUTHENTICATED,
    CONFIGURING,CONFIGURED,
    CLOSING,CLOSE,CLOSED
}
