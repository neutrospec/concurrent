package com.unocult.common.concurrent.io;

import com.unocult.common.concurrent.LWActorRef;
import com.unocult.common.concurrent.io.nio.Connection;

public class ConnectionEntry {
    public final Connection connection;
    public final LWActorRef tcpConnection;

    public ConnectionEntry(Connection connection, LWActorRef tcpConnection) {
        this.connection = connection;
        this.tcpConnection = tcpConnection;
    }
}
