package com.unocult.common.concurrent.io;

import com.unocult.common.concurrent.LWActorRef;

public class ClientEntry {
    public final LWActorRef actor;
    public final TCP.Connect connect;

    public ClientEntry(LWActorRef actor, TCP.Connect connect) {
        this.actor = actor;
        this.connect = connect;
    }
}
