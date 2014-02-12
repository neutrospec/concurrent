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
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.unocult.common.base.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NIOOutSelector extends AbstractSelector{
	private static Logger log = LoggerFactory.getLogger(NIOOutSelector.class);

    private final Lock workLock = new ReentrantLock();
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
        workLock.lock();
        try {
            workList.add(connectWorker);
        } finally {
            workLock.unlock();
        }
	}
    private void removeWork(ConnectWorker worker) {
        workLock.lock();
        try {
            workList.remove(worker);
        } finally {
            workLock.unlock();
        }
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
				connectionManager.onConnectionFailed(worker.address, Optional.<Throwable>of(new IOException("timeout")));
			} else if (worker.isConnected()) {
				it.remove();
			}
		}
	}
	
	private Address localAddress;
		
	private class ConnectWorker implements Runnable, SelectionHandler {
		private static final long DEFAULT_TIMEOUT = 30 * 1000;
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
                removeWork(this);
				connectionManager.onConnectionFailed(address, Optional.of(e));
			}
		}

		public void handle() {
			try {
				connected = socketChannel.finishConnect();
				if (!connected) {
					log.error("FATAL condition: failed to establish remote connection but is this possible? {}", address);
                    throw new IOException("internal error. finishConnect should throw exception or return true.");
                }
				log.debug("connected to: {}", address);
                deregister();
				removeWork(ConnectWorker.this);
				connection = createConnection(socketChannel);
			} catch (Throwable e) {
				log.warn("connection worker failed: " + address.toString(), e);
				try {socketChannel.close(); } catch (Exception ignore) {}
                removeWork(ConnectWorker.this);
				connectionManager.onConnectionFailed(address, Optional.of(e));
			}
		}
	}
}
