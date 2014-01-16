package com.unocult.common.concurrent.io.nio;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Hashtable;

public class NIOAcceptSelector extends AbstractSelector {
    private static Logger logger = LoggerFactory.getLogger(NIOAcceptSelector.class);

    private final Hashtable<InetSocketAddress, BindEntry> serverChannels = new Hashtable<InetSocketAddress, BindEntry>();

    public NIOAcceptSelector(ConnectionManager connectionManager) {
        this.connectionManager = connectionManager;
    }

    public boolean bind(InetSocketAddress bindAddress) throws IOException {
        logger.debug("begin bind: {}", bindAddress);
        if (serverChannels.contains(bindAddress)) {
            throw new IOException("Bind address duplicated: " + bindAddress);
        }
        final ServerSocketChannel channel = ServerSocketChannel.open();
        int size = connectionManager.getServerSocketReceiveBufferSize();
        if (size > ConnectionManager.MIN_BUFFER_SIZE)
            channel.socket().setReceiveBufferSize(0); // FIXME: override default buffer?
        channel.bind(bindAddress);
        channel.configureBlocking(false);
        SelectionKey key = channel.register(selector, SelectionKey.OP_ACCEPT, new AcceptHandler(channel));
        BindEntry entry = new BindEntry(channel, key);
        serverChannels.put(bindAddress, entry);
        selector.wakeup();
        logger.info("Ready to accept connections: receive buffer = " + channel.socket().getReceiveBufferSize());
        return true;
    }

    private class AcceptHandler implements SelectionHandler {
        final ServerSocketChannel channel;
        AcceptHandler(ServerSocketChannel channel) {
            this.channel = channel;
        }
        public void handle() {
            try {
                SocketChannel client = channel.accept();
                logger.info("connection accepted from: port-{} {}",
                    client.socket().getLocalPort(),
                    client.socket().getRemoteSocketAddress());
                try {
                    final Connection connection = createConnection(client);

                } catch (Exception e) {
                    logger.error("Socket error", e);
                }
            } catch (Throwable e) {
                logger.error("server socket error", e);
            }
        }
    }
    protected void process(SelectionKey key) throws IOException {
        AcceptHandler handler = (AcceptHandler) key.attachment();
        if (handler == null) {
            logger.error("invalid key attachment!!");
            return;
        }
        handler.handle();
    }

    public void shutdown() {
        logger.info("Accept Selector shutdown initiated.");
        super.shutdown();
        for (BindEntry entry: serverChannels.values()) {
            logger.info("Closing listen port: {}", entry.channel.socket().getLocalPort());
            try {
                entry.channel.close();
            } catch (Exception ignore) {
            }
        }
        logger.info("Accept Selector closed.");
    }

    class BindEntry {
        final ServerSocketChannel channel;
        final SelectionKey key;

        BindEntry(ServerSocketChannel channel, SelectionKey key) {
            this.channel = channel;
            this.key = key;
        }
    }
}
