package com.unocult.common.concurrent.io;

import com.unocult.common.annotations.GuardedBy;
import com.unocult.common.base.Optional;
import com.unocult.common.concurrent.ConcurrentSystem;
import com.unocult.common.concurrent.LWActorRef;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

public class TCP {
    private static Object monitor = new Object();
    @GuardedBy("monitor")
    private static LWActorRef manager = null;

    public static LWActorRef getManager() {
        synchronized (monitor) {
            if (manager == null)
                createManager();
        }
        return manager;
    }
    private static void createManager() {
        ConcurrentSystem system = ConcurrentSystem.getConcurrentSystem();
        manager = system.actorOf(TCPManager.class);
    }
    public interface TCPCommand {}
    public static class Connect implements TCPCommand {
        public Connect(InetSocketAddress remote) {
            this.remote = remote;
        }
        public final InetSocketAddress remote;
    }
    public static class Bind implements TCPCommand {
        public final LWActorRef actor;
        public final InetSocketAddress bindAddress;
        public Bind(LWActorRef actor, InetSocketAddress address) {
            this.actor = actor;
            this.bindAddress = address;
        }
    }
    public static class Bound implements TCPCommand {

    }
    public static class CommandFailed implements TCPCommand {
        public final Object command;
        public final Optional<Throwable> cause;
        public CommandFailed(Object command, Optional<Throwable> cause) {
            this.command = command;
            this.cause = cause;
        }
    }
    public static class Connected implements TCPCommand {
        public final InetSocketAddress remote;
        public final InetSocketAddress local;

        public Connected(InetSocketAddress remote, InetSocketAddress local) {
            this.remote = remote;
            this.local = local;
        }
    }
    public static class Close implements TCPCommand {

    }
    public static class CloseSafe implements TCPCommand {

    }
    public static class Closed implements TCPCommand {
        public final InetSocketAddress remote;
        public final InetSocketAddress local;
        public Closed(InetSocketAddress remote, InetSocketAddress local) {
            this.remote = remote;
            this.local = local;
        }
    }
    public static class Register implements TCPCommand {
        public final LWActorRef actor;
        public Register(LWActorRef actor) {
            this.actor = actor;
        }
    }

    public static class Received implements TCPCommand {
        public final ByteBuffer data;
        public Received(ByteBuffer data) {
            this.data = data;
        }
    }

    public static class Write implements TCPCommand {
        public final ByteBuffer data;
        public Write(ByteBuffer data) {
            this.data = data;
        }
    }

    public static class WriteAck implements TCPCommand {
        public final Write data;
        public WriteAck(Write data) {
            this.data = data;
        }
    }
}
