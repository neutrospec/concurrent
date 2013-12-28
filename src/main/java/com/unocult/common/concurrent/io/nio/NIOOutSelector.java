package com.unocult.common.concurrent.io.nio;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NIOOutSelector extends AbstractSelector{
	private static Logger log = LoggerFactory.getLogger(NIOOutSelector.class);
	private List<ConnectWorker> workList = new Vector<ConnectWorker>();
	
	public NIOOutSelector(ConnectionManager connectionManager) {
		this.connectionManager = connectionManager;
		try {
			localAddress = new Address(new InetSocketAddress(InetAddress.getLocalHost(), 0));
		} catch (UnknownHostException u) {
			log.error("cannot determine who am i", u);
		}
	}
	public void connect(Address address) {
		ConnectWorker connectWorker = new ConnectWorker(address);
		addTask(connectWorker);
		selector.wakeup();
		workList.add(connectWorker);
	}

    @Override
    protected void process(SelectionKey key) throws IOException {
        ConnectWorker worker = (ConnectWorker) key.attachment();
        if (worker != null)
            worker.handle();
    }

    void checkTimeout() {
		Iterator<ConnectWorker> it = workList.iterator();
		while (it.hasNext()) {
			ConnectWorker worker = it.next();
			if (!worker.isConnected() && worker.isTimeout()) {
				worker.deregister();
				it.remove();
				connectionManager.onConnectionFailed(worker.address);
			} else if (worker.isConnected()) {
				it.remove();
			}
		}
	}
	
	private Address localAddress;
		
	private class ConnectWorker implements Runnable, SelectionHandler {
		private static final long DEFAULT_TIMEOUT = 10 * 1000;
		SocketChannel socketChannel;
		Address address;
		Connection connection;
		volatile boolean connected = false;
		long timeout;
		public ConnectWorker(Address address) {
			this(address, DEFAULT_TIMEOUT);
		}
		public ConnectWorker(Address address, long timeout) {
			this.address = address;
			this.timeout = System.currentTimeMillis() + timeout;
		}
		boolean isConnected() {
			return connected;
		}
		boolean isTimeout() {
			return (timeout - System.currentTimeMillis()) < 0; 
		}
		void deregister() {
			SelectionKey key = socketChannel.keyFor(selector);
			if (key != null)
				key.cancel();
		}
		public void run() {
			try {
				socketChannel = SocketChannel.open();
				try {
//					socketChannel.socket().bind(new InetSocketAddress(localAddress.getInetAddress(), 0));
					socketChannel.configureBlocking(false);
					log.info("connecting to: {}", address);
					connected = socketChannel.connect(address.getInetSocketAddress());
					if (connected) { 
						handle();
						return;
					}
				} catch (Throwable e) {
					log.warn("cannot init socket", e);
				}
				socketChannel.register(selector, SelectionKey.OP_CONNECT, ConnectWorker.this);
			} catch (Throwable e) {
				try {socketChannel.close();} catch (Exception ignore) {}
				connectionManager.onConnectionFailed(address);
			}
		}

		public void handle() {
			try {
				connected = socketChannel.finishConnect();
				if (!connected) {
					socketChannel.register(selector, SelectionKey.OP_CONNECT, ConnectWorker.this);
					log.info("failed to connect :");
					return;
				}
				log.debug("connected to: {}", address);
				connection = createConnection(socketChannel);
				
			} catch (Throwable e) {
				log.warn("connection worker failed: ", e);
				try {socketChannel.close(); } catch (Exception ignore) {}
				connectionManager.onConnectionFailed(address);
			}
		}
	}
}
