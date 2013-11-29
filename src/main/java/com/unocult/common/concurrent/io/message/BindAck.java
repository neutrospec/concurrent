package com.unocult.common.concurrent.io.message;

import com.unocult.common.base.Optional;

import java.net.InetSocketAddress;

public class BindAck {
    public InetSocketAddress bindAddress;
    public boolean success;
    public Optional<Throwable> error;

    public BindAck(InetSocketAddress bindAddress, boolean success, Optional<Throwable> error) {
        this.bindAddress = bindAddress;
        this.success = success;
        this.error = error;
    }
}
