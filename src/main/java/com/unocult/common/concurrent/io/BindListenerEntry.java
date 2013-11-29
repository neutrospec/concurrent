package com.unocult.common.concurrent.io;


import com.unocult.common.concurrent.LWActorRef;

import java.net.InetSocketAddress;

public class BindListenerEntry {
    public LWActorRef listener;
    public InetSocketAddress bindAddress;
    private boolean bound = false;
    public BindListenerEntry(LWActorRef listener, InetSocketAddress address) {
        this.listener = listener;
        this.bindAddress = address;
    }

    public boolean isBound() {
        return bound;
    }

    public void setBound(boolean bound) {
        this.bound = bound;
    }
}
