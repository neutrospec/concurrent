package com.unocult.common.concurrent.io.nio;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.Queue;
import java.util.Vector;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.unocult.common.base.Optional;
import com.unocult.common.concurrent.LWActorRef;
import com.unocult.common.concurrent.io.message.Connected;
import com.unocult.common.concurrent.io.message.ConnectionClosed;
import com.unocult.common.concurrent.io.message.ConnectionFailed;
import com.unocult.common.concurrent.io.message.SocketProblem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConnectionManager {
	private static Logger log = LoggerFactory.getLogger(ConnectionManager.class);
    // TCP Manager actor
    private LWActorRef owner;
    private static final int defaultBacklog = 1024;
    public static final int MIN_BUFFER_SIZE = 512;
    public static final int cpuCount = Runtime.getRuntime().availableProcessors();
    private SocketOption socketOption;

    private NIOAcceptSelector acceptSelector = new NIOAcceptSelector(this);

    private NIOInSelector[] selectors = new NIOInSelector[cpuCount];
    private NIOOutSelector outSelector;
	
	private AtomicLong connectionIDGen = new AtomicLong(1);
    private int backlog = defaultBacklog;
    private ByteBufferCache directBufferCache = new ByteBufferCache(100);

	private List<Connection> connectionList = new Vector<Connection>();
	private List<Address> connectionInConnectiongList = new Vector<Address>();
	
	private ScheduledExecutorService managementSchedule = Executors.newSingleThreadScheduledExecutor();

    public ConnectionManager(LWActorRef owner) {
        this.owner = owner;
    }

	public void setSocketOption(SocketOption socketOption) {
        this.socketOption = socketOption;
    }

    public boolean isBundle() {
        if (socketOption == null)
            return true;
        return socketOption.isBundle();
    }

	public void start() throws IOException {
        Thread at = new Thread(acceptSelector, "Accept Selector Thread.");
        at.start();
        for (int i = 0; i < cpuCount; i++) {
            NIOInSelector sel = new NIOInSelector(this);
            selectors[i] = sel;
		    Thread it = new Thread(sel, "Selector Thread: "+i);
            it.start();
        }
        outSelector = new NIOOutSelector(this);
        Thread ot = new Thread(outSelector, "Client Selector Thread");
        ot.start();
//        try {Thread.sleep(2000);} catch (Exception ignore) {}

		managementSchedule.scheduleWithFixedDelay(new Runnable() {
			public void run() {
				outSelector.checkTimeout();
			}
		}, 1, 1, TimeUnit.SECONDS);
	}

	protected NIOInSelector allocateSelector(Connection connection) {
        int index = (int) (connection.getConnectionID() % cpuCount);
        NIOInSelector s = selectors[index];
        return s;
    }

	public void shutdown() {
		managementSchedule.shutdownNow();
        acceptSelector.shutdown();
        for (NIOInSelector selector : selectors) {
            selector.shutdown();
        }
		outSelector.shutdown();
	}
	public void connect(Address address) {
		connectionInConnectiongList.add(address);
		outSelector.connect(address);
	}

    public boolean bind(InetSocketAddress address) throws IOException{
        return acceptSelector.bind(address);
    }

    /**
     * create connection with given socket channel.
     * returned connection is not registered to any read selecctors.
     * call registerConnection before read/write.
     *
     * @param socketChannel accepted/connected SocketChannel
     * @return Connection which is unregistered.
     */
	Connection createConnection(SocketChannel socketChannel) throws IOException {
        return createConnection(socketChannel, null);
    }

    /**
     * create connection with given socket channel.
     * returned connection is not registered to any read selecctors.
     * call registerConnection before read/write.
     *
     * @param socketChannel accepted/connected SocketChannel
     * @return Connection which is unregistered.
     */
	Connection createConnection(SocketChannel socketChannel, LWActorRef sender) throws IOException {
        try {
            changeSocketOption(socketChannel);
            long nextConnectionID = connectionIDGen.getAndIncrement();
            Connection conn = new Connection(this, socketChannel, nextConnectionID);
            conn.setSocketWriteBuffer(directBufferCache.getDirectByteBuffer(getSocketWriteFragmentSize()));
            conn.setSocketReadBuffer(directBufferCache.getDirectByteBuffer(getSocketWriteFragmentSize()));
            connectionList.add(conn);
            // toss Connection to TCP manager.
            owner.send(new Connected(conn, (InetSocketAddress)socketChannel.getRemoteAddress(), (InetSocketAddress)socketChannel.getLocalAddress(), sender));
            log.info("connection established - remote: {}", socketChannel.socket().getRemoteSocketAddress());
            return conn;
        } catch (IOException e) {
            log.error("invalid socket channel", e);
            throw e;
        }

	}

    /**
     * register connection to InSelector.
     *
     * @param conn
     */
    public void registerConnection(Connection conn) {
        NIOInSelector s = allocateSelector(conn);
        conn.setSelector(s);
        s.addTask(conn.getReadHandler());
        s.wakeup();
    }

    int getSocketWriteFragmentSize() {
        return (socketOption != null) ? socketOption.getConnectionBufferSize() : SocketOption.DEFAULT_WRITE_FRAGMENT_SIZE;
    }
    private void changeSocketOption(SocketChannel socketChannel) {
        try {
            socketChannel.configureBlocking(false);
            socketChannel.socket().setKeepAlive(true);
            socketChannel.socket().setTcpNoDelay(true);
            if (socketOption != null) {
                if (socketOption.getReceiveBufferSize() >= MIN_BUFFER_SIZE)
                    socketChannel.socket().setReceiveBufferSize(socketOption.getReceiveBufferSize());
                if (socketOption.getSendBufferSize() >= MIN_BUFFER_SIZE)
                    socketChannel.socket().setSendBufferSize(socketOption.getSendBufferSize());
            }
        } catch (IOException ignore) {}
    }

	void onConnectionFailed(Address address, Optional<Throwable> cause) {
        owner.send(new ConnectionFailed(address.getInetSocketAddress(), cause, (LWActorRef) address.attach));
	}
	void onSocketException(Connection connection, Throwable e) {
        Optional<LWActorRef> conActor = connection.getOwner();
        if (conActor.isPresent()) {
            conActor.get().send(new SocketProblem(e));
        } else {
            log.warn("connection with exception has no TCP connection actor");
        }
        log.warn("Socket Exception", e);
	}
	void closeConnection(Connection connection) {
		log.info("connection closed - remote: {}", connection.getSocketChannel().socket().getRemoteSocketAddress());
		try {connection.getSocketChannel().close();} catch (Throwable ignore) {}
        directBufferCache.releaseDirectByteBuffer(connection.getSocketReadBuffer());
        directBufferCache.releaseDirectByteBuffer(connection.getSocketWriteBuffer());
        connectionList.remove(connection);
        owner.send(new ConnectionClosed(connection));
	}
    public int getConnectionCount() {
        return connectionList.size();
    }

    public List<Connection> getConnections() {
        // TODO: return copied list
        return connectionList;
    }

    protected int getServerSocketReceiveBufferSize() {
        if (socketOption == null)
            return 0;
        return socketOption.getReceiveBufferSize();
    }
}
