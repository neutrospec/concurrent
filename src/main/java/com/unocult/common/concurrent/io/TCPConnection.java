package com.unocult.common.concurrent.io;

import com.unocult.common.base.Optional;
import com.unocult.common.concurrent.ConcurrentSystem;
import com.unocult.common.concurrent.LWActor;
import com.unocult.common.concurrent.LWActorRef;
import com.unocult.common.concurrent.io.message.Connected;
import com.unocult.common.concurrent.io.nio.Connection;
import com.unocult.common.concurrent.io.nio.ConnectionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

public class TCPConnection extends LWActor {
    private static final Logger logger = LoggerFactory.getLogger(TCPConnection.class);
    private final Connection connection;
    private final LWActorRef requester;
    private final ConnectionManager connectionManager;

    private Optional<LWActorRef> listener = Optional.absent();
    protected boolean listenerDead = false;
    private InetSocketAddress remote;
    private InetSocketAddress local;

    public TCPConnection(ConnectionManager connectionManager, Connected connected, LWActorRef ref) {
        this.requester = ref;
        this.connection = connected.connection;
        this.connectionManager = connectionManager;
        remote = connected.remote;
        local = connected.local;
    }

    @Override
    protected void preStart() {
        connection.setOwner(Optional.of(self));
        requester.send(new TCP.Connected(remote, local));
        self.watch(requester);
    }

    @Override
    protected boolean receive(Object message) {
        logger.debug("CM: {}", message);
        if (message instanceof TCP.Register) {
            register((TCP.Register) message);
        } else if (message instanceof TCP.Write) {
            write((TCP.Write) message);
        } else if (message instanceof TCP.Received) {
            read((TCP.Received) message);
        } else if (message instanceof TCP.WriteAck) {
            sendWriteAck((TCP.WriteAck) message);
        } else if (message instanceof TCP.Closed) {
            closed((TCP.Closed) message);
        } else if (message instanceof TCP.Close) {
            closeConn((TCP.Close) message);
        } else if (message instanceof ConcurrentSystem.Terminated) {
            closeConn((ConcurrentSystem.Terminated) message);
        } else {
            self.getSystem().deadLetter(self, message);
        }
        return true;
    }
    private void register(TCP.Register message) {
        listener = Optional.of(message.actor);
        self.unwatch(requester);
        self.watch(message.actor);
        connectionManager.registerConnection(connection);
    }
    private void write(TCP.Write message) {
        connection.write(message);
    }
    private void sendWriteAck(TCP.WriteAck message) {
        if (!sendToListener(message))
            logger.warn("received write ack from TCP connection without Register");
    }
    private void read(TCP.Received message) {
        if (!sendToListener(message))
            logger.warn("received packet from TCP connection without Register!!");
    }
    private void closed(TCP.Closed message) {
        if (!sendToListener(message) && !listenerDead)
            logger.warn("unregistered connection closed");
    }
    protected boolean sendToListener(Object message) {
        if (listener.isPresent()) {
            listener.get().send(message);
            logger.debug("sent message: {}", message);
            return true;
        }
        return false;
    }
    protected void closeConn(TCP.Close message) {
        connection.close();
        sendToListener(new TCP.Closed(remote, local));
    }
    protected void closeConn(ConcurrentSystem.Terminated message) {
        listener = Optional.absent();
        listenerDead = true;
        connection.close();
    }
}
