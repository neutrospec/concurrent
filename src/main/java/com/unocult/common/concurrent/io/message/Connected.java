package com.unocult.common.concurrent.io.message;

import com.unocult.common.concurrent.LWActorRef;
import com.unocult.common.concurrent.io.nio.Connection;

import java.net.InetSocketAddress;

public class Connected {
    public final Connection connection;
    public final InetSocketAddress remote;
    public final InetSocketAddress local;
    public final LWActorRef sender;
    public Connected(Connection connection, InetSocketAddress remote, InetSocketAddress local, LWActorRef sender) {
        this.connection = connection;
        this.remote = remote;
        this.local = local;
        this.sender = sender;
    }
}
