package com.unocult.common.concurrent.io.message;

import com.unocult.common.base.Optional;
import com.unocult.common.concurrent.LWActorRef;

import java.net.InetSocketAddress;

public class ConnectionFailed {
    public final InetSocketAddress remote;
    public final Optional<Throwable> cause;
    public final LWActorRef sender;
    public ConnectionFailed(InetSocketAddress remote, Optional<Throwable> cause, LWActorRef sender) {
        this.remote = remote;
        this.cause = cause;
        this.sender = sender;
    }
}
