package com.unocult.common.concurrent.io.message;

import java.net.InetSocketAddress;

public class ConnectionFailed {
    public final InetSocketAddress remote;

    public ConnectionFailed(InetSocketAddress remote) {
        this.remote = remote;
    }
}
