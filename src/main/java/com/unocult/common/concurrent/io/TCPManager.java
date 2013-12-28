package com.unocult.common.concurrent.io;

import com.unocult.common.base.Optional;
import com.unocult.common.concurrent.LWActor;
import com.unocult.common.concurrent.LWActorRef;
import com.unocult.common.concurrent.io.message.Connected;
import com.unocult.common.concurrent.io.message.ConnectionClosed;
import com.unocult.common.concurrent.io.message.ConnectionFailed;
import com.unocult.common.concurrent.io.nio.Address;
import com.unocult.common.concurrent.io.nio.Connection;
import com.unocult.common.concurrent.io.nio.ConnectionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class TCPManager extends LWActor {
    private static final Logger logger = LoggerFactory.getLogger(TCPManager.class);

    private ConnectionManager connectionManager;

    private final List<BindListenerEntry> servers = new LinkedList<BindListenerEntry>();
    private final List<ClientEntry> clients = new LinkedList<ClientEntry>();
    private final Map<Long, ConnectionEntry> connections = new Hashtable<Long, ConnectionEntry>();

    @Override
    protected void preStart() {
        connectionManager = new ConnectionManager(self);
        try {
            connectionManager.start();
        } catch (Exception e) {
            logger.error("TCPManager startup failed.", e);
        }
    }

    @Override
    protected boolean receive(Object message) {
        logger.debug("Received: {}", message);
        if (message instanceof TCP.Bind) {
            bind((TCP.Bind) message);
        } else if (message instanceof TCP.Connect) {
            connect((TCP.Connect) message);
        } else if (message instanceof Connected) {
            connected((Connected) message);
        } else if (message instanceof ConnectionClosed) {
            closed((ConnectionClosed) message);
        } else if (message instanceof ConnectionFailed) {
            connectionFailed((ConnectionFailed) message);
        } else {
            logger.error("unprocessed message: {}", message);
        }

        return true;
    }

    protected void bind(TCP.Bind m) {
        LWActorRef listener = m.actor;
        try {
            connectionManager.bind(m.bindAddress);
            servers.add(new BindListenerEntry(listener, m.bindAddress));
            listener.send(new TCP.Bound());
        } catch (Throwable t) {
            logger.error("Bind error [{}]", m.bindAddress, t);
            listener.send(new TCP.CommandFailed(m, Optional.of(t)));
        }
    }

    protected void connected(Connected connected) {
        Optional<BindListenerEntry> server = findServer(connected.local);
        if (server.isPresent()) {
            createTCPConnection(connected, server.get().listener);
        } else {
            Optional<ClientEntry> client = findClient(connected.remote);
            if (client.isAbsent()) {
                connected.connection.close();
                logger.error("unexpected connection destroyed: [{}] -> [{}]", connected.local, connected.remote);
            } else {
                clients.remove(client.get());
                createTCPConnection(connected, client.get().actor);
            }
        }
    }

    protected void connectionFailed(ConnectionFailed connectionFailed) {
        Optional<ClientEntry> client = findClient(connectionFailed.remote);
        if (client.isAbsent()) {
            logger.error("unexpected connection destroyed: -> [{}]", connectionFailed.remote);
        } else {
            clients.remove(client.get());
            client.get().actor.send(new TCP.CommandFailed(client.get().connect, Optional.<Throwable>absent()));
        }
    }

    protected void closed(ConnectionClosed message) {
        Optional<BindListenerEntry> server = findServer(message.connection.local);
        if (server.isPresent()) {
            servers.remove(server);
            logger.info("Server socket closed: {}", message.connection.local);
        } else {
            Optional<ClientEntry> client = findClient(message.connection.remote);
            if (client.isPresent()) {
                logger.warn("Connection connection received unexpected close event: {}", client.get().connect.remote);
                client.get().actor.send(new TCP.CommandFailed(client.get().connect, Optional.<Throwable>absent()));
                return;
            }
            ConnectionEntry entry = connections.remove(message.connection.getConnectionID());
            if (entry != null) {
                entry.tcpConnection.send(new TCP.Closed(message.connection.remote, message.connection.local));
                logger.debug("connection [{} {}] closed", entry.connection.getConnectionID(), entry.connection.remote);
            }
        }
    }
    protected void connect(TCP.Connect message) {
        try {
            clients.add(new ClientEntry(sender.get(), message));
            connectionManager.connect(new Address(message.remote));
            logger.debug("connecting to: {}", message.remote);
        } catch (Exception e) {
            logger.error("connect error", e);
        }
    }

    protected Optional<BindListenerEntry> findServer(InetSocketAddress local) {
        for (BindListenerEntry e: servers) {
            if (e.bindAddress.getPort() == local.getPort())
                return Optional.of(e);
        }
        return Optional.absent();
    }

    protected Optional<ClientEntry> findClient(InetSocketAddress remote) {
        for (ClientEntry c: clients) {
            if (c.connect.remote.equals(remote))
                return Optional.of(c);
        }
        return Optional.absent();
    }

    /**
     * create Actor for each tcp connection.
     * created actor will send TCP.Connected message to ref.
     *
     * @param conn connected message (from ConnectionManager)
     * @param ref actor who will receive TCP.Connected message
     * @return
     */
    protected LWActorRef createTCPConnection(Connected conn, LWActorRef ref) {
        LWActorRef connRef = getSystem().actorOf(TCPConnection.class, connectionManager, conn, ref);
        ConnectionEntry entry = new ConnectionEntry(conn.connection, connRef);
        connections.put(conn.connection.getConnectionID(), entry);
        return connRef;
    }
}
