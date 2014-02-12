package com.unocult.common.concurrent.io.message;

import com.unocult.common.base.Optional;

import java.net.InetSocketAddress;

public class ConnectionFailed {
    public final InetSocketAddress remote;
    public final Optional<Throwable> cause;
    public ConnectionFailed(InetSocketAddress remote, Optional<Throwable> cause) {
        this.remote = remote;
        this.cause = cause;
    }
}
