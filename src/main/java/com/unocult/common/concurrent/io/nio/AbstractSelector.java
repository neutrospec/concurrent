package com.unocult.common.concurrent.io.nio;

import java.io.IOException;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.unocult.common.concurrent.LWActorRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract public class AbstractSelector implements Runnable {
	private static Logger log = LoggerFactory.getLogger(AbstractSelector.class);
    protected static int socketReceiveBufferSize = 32 * Constants.KBYTE;
    protected Selector selector;
    protected boolean run;
	private long waitTime = 100;
	private long shutdownTimeout = 3;
	protected ConnectionManager connectionManager;
	private int packetPoolMaxSize = 3000;
	public AbstractSelector() {
		try {
			selector = Selector.open();
		} catch (IOException e) {
			log.error("Selector open failed", e);
		}
		run = true;
	}
	public void setConnectionManager(ConnectionManager connectionManager) {
		this.connectionManager = connectionManager;
	}
	public int getSocketReceiveBufferSize() { return socketReceiveBufferSize; }
	public void run() {
		try {
			while (run) {
				processTaskQueue();
				if (!run)
					return;
				int available = -1;
				try {
					available = selector.select(waitTime);
				} catch (Throwable ignore) {
					continue;
				}
				if (available == 0)
					continue;
				Set<SelectionKey> keys = selector.selectedKeys();
				Iterator<SelectionKey> iter = keys.iterator();
                int cancelled = 0;
				while (iter.hasNext()) {
					SelectionKey key = iter.next();
					try {
                        process(key);
                    } catch (CancelledKeyException e) {
                        cancelled ++;
                        if (cancelled > 250)
                            break;
					} catch (Throwable t) {
						log.warn("Selection handling error.", t);
                    } finally {
                        iter.remove();
                    }
                }
			}
		} catch (Throwable e) {
			log.error("Unknown error", e);
		} finally {
			log.info("Closing selector: {}", Thread.currentThread().getName());
			try {selector.close();} catch (Exception ignore) {}
		}
	}

    protected abstract void process(SelectionKey key) throws IOException;

    protected void clearWriteOp(SelectionKey key) {
        try {
            key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
        } catch (Throwable t) {
            log.warn("key operation error", t);
        }
    }

    protected void registerWriteOp(SelectionKey key) {
        try {
            key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
        } catch (Throwable t) {
            log.warn("key operation error", t);
        }
    }

	protected Connection createConnection(SocketChannel channel) throws IOException {
		Connection connection = connectionManager.createConnection(channel);
		return connection;
	}

	protected Connection createConnection(SocketChannel channel, LWActorRef sender) throws IOException {
		Connection connection = connectionManager.createConnection(channel, sender);
		return connection;
	}
	protected SelectionKey registerOp(Connection connection, int operation) {
		SelectionKey key = null;
		try {
		    SocketChannel channel = connection.getSocketChannel();
			key = channel.register(selector, operation, connection);
		} catch (Throwable t) {
			log.info("failed to register socket channel", t);
			onSocketException(t);
		}
		return key;
	}
	private Queue<Runnable> taskQueue = new ConcurrentLinkedQueue<Runnable>();
	public int addTask(Runnable task) {
		taskQueue.add(task);
		return taskQueue.size();
	}
	protected void clearTaskQueue() {
		taskQueue.clear();
	}
	protected void processTaskQueue() {
		while (true) {
			Runnable run = taskQueue.poll();
			if (run == null)
				break;
			try {
                run.run();
            } catch (Throwable e) {
                log.error("Error in task processing.", e);
            }

		}
	}
	public void shutdown() {
		clearTaskQueue();
		final CountDownLatch latch = new CountDownLatch(1);
		addTask(new Runnable() {
			public void run() {
				run = false;
				latch.countDown();
			}
		});
		wakeup();
		try {latch.await(shutdownTimeout, TimeUnit.SECONDS);} catch (InterruptedException ignore) {}
	}
	protected void onSocketException(Throwable t) {
	}
	public void wakeup() {
		selector.wakeup();
	}
	public Selector getSelector() {
		return selector;
	}
}
