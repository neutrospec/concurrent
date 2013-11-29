package com.unocult.common.concurrent.io.nio;

import com.unocult.common.base.Optional;
import com.unocult.common.concurrent.LWActorRef;
import com.unocult.common.concurrent.io.TCP;
import com.unocult.common.concurrent.io.nio.buffer.DoubleBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.Hashtable;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class Connection {
	private static Logger log = LoggerFactory.getLogger(Connection.class);
    protected ConnectionManager connectionManager;
    protected SocketChannel socketChannel;
    protected NIOInSelector selector;
    protected SelectionKey key;
    private Optional<LWActorRef> owner = Optional.absent();
    protected volatile boolean connected = true;

	protected long connectionID;

	private WriteHandler writeHandler;
	private ReadHandler readHandler;

    private final Map<Object, Object> attributeMap = new Hashtable<Object, Object>();

    public final InetSocketAddress remote;
    public final InetSocketAddress local;

    private long startTime;
    private int sentMessageCount = 0;
    private int receivedMessageCount = 0;

    private DoubleBuffer receiveBuffer;
    private DoubleBuffer sendBuffer;

    public Connection(ConnectionManager connectionManager, SocketChannel socketChannel, long connectionID) throws IOException{
		this.connectionManager = connectionManager;
		this.socketChannel = socketChannel;
		this.connectionID = connectionID;
        this.remote = (InetSocketAddress) socketChannel.getRemoteAddress();
        this.local = (InetSocketAddress) socketChannel.getLocalAddress();
        writeHandler = new WriteHandler();
        readHandler = new ReadHandler();

        startTime = System.currentTimeMillis();
	}
	public long getConnectionID() {return connectionID; }
	public SocketChannel getSocketChannel() {return socketChannel; }

    public NIOInSelector getSelector() {
        return selector;
    }

    public void setSelector(NIOInSelector selector) {
        this.selector = selector;
    }

    protected void doRead() {
        readHandler.handle();
    }

    protected void doWrite() {
        writeHandler.handle();
    }

	public void write(TCP.Write packet) {
		writeHandler.writeFromWorkerThread(packet);
        increaseSentCount();
	}

	public synchronized void close() {
        if (!isConnected())
            return;
		connected = false;
		connectionManager.closeConnection(this);
		writeHandler.writeQueue.clear();
	}
    public boolean isConnected() {
        return connected;
    }
	public ReadHandler getReadHandler() {
		return readHandler;
	}
	public SelectionHandler getWriteHandler() {
		return writeHandler;
	}

    public ByteBuffer getSocketWriteBuffer() {
        return sendBuffer.rawBuffer();
    }

    public void setSocketWriteBuffer(ByteBuffer socketWriteBuffer) {
        this.sendBuffer = new DoubleBuffer(socketWriteBuffer);

    }

    public ByteBuffer getSocketReadBuffer() {
        return receiveBuffer.rawBuffer();
    }

    public void setSocketReadBuffer(ByteBuffer socketReadBuffer) {
        this.receiveBuffer = new DoubleBuffer(socketReadBuffer);
    }

    private class WriteHandler implements SelectionHandler, Runnable {
        protected static final int WRITE_QUEUE_CAPACITY = 1000;
        protected static final int WRITE_SPIN = 3;
        Lock writeLock = new ReentrantLock();
        protected volatile boolean writable = false;
        public AtomicBoolean isNotRegistered = new AtomicBoolean(true);
		private Optional<TCP.Write> currentWritePacket = Optional.absent();
        protected final BlockingQueue<TCP.Write> writeQueue = new LinkedBlockingQueue<TCP.Write>(WRITE_QUEUE_CAPACITY);

        public void run() {
			isNotRegistered.set(true);
			if (writable)
				writeFromIOThread();
			else
				turnOnWriteOp();
			writable = false;
		}

		public void handle() {
            writeFromIOThread();
		}
        protected void writeFromWorkerThread(TCP.Write packet) {
            if (!isConnected())
                return;
            boolean full = true;
            writeLock.lock();
            try {
                if (writeQueue.isEmpty() && currentWritePacket.isAbsent()) {
                    try {
                        fillSocketWriteBufferWithSinglePacket(packet);
                        full = tryToWriteWriteBufferToSocketUntilSpinCount();
                    } catch (Throwable t) {
                        log.warn("write handler error", t);
                    }
                } else {
                    boolean offer = false;
                    try {offer = writeQueue.offer(packet, 100, TimeUnit.MILLISECONDS); } catch (InterruptedException ignore) {}
                    if (!offer)
                        close();
                }
            } finally {
                if (full && isConnected()) {
                    if (writeHandler.isNotRegistered.compareAndSet(true, false)) {
                        selector.addTask(WriteHandler.this);
                        selector.wakeup();
                    } else {
                        // job exists in task queue
                    }
                }
                writeLock.unlock();
            }
        }

        private boolean tryToWriteWriteBufferToSocketUntilSpinCount() throws IOException {
            boolean full = true;
            for (int i = 0; i < WRITE_SPIN; i++) {
                int written = writeWriteBufferToSocket();
                if (written > 0) {
                    try {
                        if (sendBuffer.readableSize() == 0 && currentWritePacket == null) {
                            return false;
                        }
                        if (sendBuffer.readableSize() == 0) {
                            fillSocketWriteBufferWithPacketsInQueue();
                            continue;
                        }
                    } finally {
                        optimizeWriteBuffer();
                    }
                    return true;
                }
            }
            return full;
        }

        private void fillSocketWriteBufferWithSinglePacket(TCP.Write preparedPacket) throws IOException {
            currentWritePacket = Optional.of(preparedPacket);
            fillSocketWriteBufferWithPacketsInQueue();
        }

        protected void writeFromIOThread() {
            writeLock.lock();
            try {
                prepareCurrentWritePacket();
                if (currentWritePacket.isAbsent() && sendBuffer.readableSize() == 0) {
                    writable = true;
                    turnOffWriteOp();
                    return;
                }
                if (!isConnected())
                    return;

                try {
                    fillSocketWriteBufferWithPacketsInQueue();
                    writeWriteBufferToSocket();
                    optimizeWriteBuffer();
                } catch (Throwable e) {
                    log.warn("write handle error", e);
                } finally {
                    writable = false;
                    turnOnWriteOp();
                }
            } finally {
                writeLock.unlock();
            }
        }

        private void fillSocketWriteBufferWithPacketsInQueue() throws IOException {
            sendBuffer.beginWrite();
            try {
                ByteBuffer buff = sendBuffer.writeBuffer();
                while (buff.hasRemaining() && currentWritePacket.isPresent()) {
                    Common.fillBuffer(buff, currentWritePacket.get().data);
                    boolean completed = !currentWritePacket.get().data.hasRemaining();
                    if (completed) {
                        owner.get().send(new TCP.WriteAck(currentWritePacket.get()));
                        currentWritePacket = Optional.absent();
                        break;
                    }
                }
            } finally {
                sendBuffer.end();
            }
        }
        private boolean prepareCurrentWritePacket() {
            if (currentWritePacket.isAbsent()) {
                TCP.Write p = writeQueue.poll();
                if (p == null)
                    currentWritePacket = Optional.absent();
                else
                    currentWritePacket = Optional.of(p);
            }
            return (currentWritePacket.isPresent());
        }
        private int writeWriteBufferToSocket() throws IOException {
            sendBuffer.beginRead();
            int n = 0;
            try {
                ByteBuffer buff = sendBuffer.readBuffer();
                n = socketChannel.write(buff);
            } finally {
                sendBuffer.end();
            }
            return n;
        }
        private void optimizeWriteBuffer() {
            sendBuffer.optimize(0.9f);
        }
	}

	private class ReadHandler implements SelectionHandler, Runnable {
        public void run() {
            key = selector.registerOp(Connection.this, SelectionKey.OP_READ);
        }

        public void handle() {
			try {
                readSocketChannelToReadBuffer();
                if (isReadBufferEmpty())
                    return;
				readAndProcessPacket();
                optimizeReadBuffer();
			} catch (Throwable t) {
				log.warn("read handler exception", t);
				onSocketException(t);
			}
		}
        private void readSocketChannelToReadBuffer() throws IOException {
            receiveBuffer.beginWrite();
            int size = -1;
            try {
                ByteBuffer buff = receiveBuffer.writeBuffer();
                size = socketChannel.read(buff);
            } finally {
                receiveBuffer.end();
            }
            if (size < 0) {
                close();
            }
        }
        private void readAndProcessPacket() throws IOException {
            receiveBuffer.beginRead();
            try {
                ByteBuffer buff = receiveBuffer.readBuffer();
                ByteBuffer tmp = ByteBuffer.allocate(buff.remaining());
                Common.fillBuffer(tmp, buff);
                tmp.flip();
                owner.get().send(new TCP.Received(tmp));
                increaseReceivedCount();
            } finally {
                receiveBuffer.end();
            }
        }
        private void optimizeReadBuffer() {
            receiveBuffer.optimize(0.9f);
        }
        private boolean isReadBufferEmpty() {
            return (receiveBuffer.readableSize() == 0);
        }
	}
	private void onSocketException(Throwable e) {
		connectionManager.onSocketException(this, e);
        close();
	}
	protected void turnOnWriteOp() {
		try {
            int interestOps = key.interestOps();
			key.interestOps(interestOps | SelectionKey.OP_WRITE);
		} catch (Throwable e) {
			log.info("register write fail", e);
			close();
		}
	}
	protected void turnOffWriteOp() {
		try {
            if (key.isValid()) {
                int interstOps = key.interestOps();
                key.interestOps(interstOps & ~SelectionKey.OP_WRITE);
            } else {
                close();
            }
		} catch (Throwable e) {
			log.info("register write fail", e);
			close();
		}
	}
    public Object getAttribute(Object key) {
        return attributeMap.get(key);
    }
    public Set<Object> getAttributeKeys() {
        return attributeMap.keySet();
    }
    public Object setAttribute(Object key, Object value) {
        return attributeMap.put(key, value);
    }

    private void increaseReceivedCount() {
        receivedMessageCount ++;
    }

    private void increaseSentCount() {
        sentMessageCount ++;
    }

    public int getReceivedMessageCount() {
        return receivedMessageCount;
    }

    public int getSentMessageCount() {
        return sentMessageCount;
    }

    public long getStartTime() {
        return startTime;
    }
	public int getWriteQueueSize() {
		if (writeHandler.writeQueue != null)
			return writeHandler.writeQueue.size();
		return 0;
	}

    public Optional<LWActorRef> getOwner() {
        return owner;
    }

    public void setOwner(Optional<LWActorRef> owner) {
        this.owner = owner;
    }
}
