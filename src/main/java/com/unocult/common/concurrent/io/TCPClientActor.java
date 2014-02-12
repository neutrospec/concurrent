package com.unocult.common.concurrent.io;

import com.unocult.common.base.Optional;
import com.unocult.common.base.Tuple2;
import com.unocult.common.concurrent.LWActor;
import com.unocult.common.concurrent.LWActorRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

/**
 * TCP client actor 를 위한 base class
 * actor 가 시작되면 remote 서버로 연결하고 끊어질 경우 자동으로 재접속을 시도한다.
 * TCPClientActor 를 base 로 하면 원하는 서버에 항상 연결된 connection 을 유지할 수 있다.
 *
 * 서버로의 연결을 수동으로 맺고 싶을때는 autoConnection method 를 override 한다.
 */
public abstract class TCPClientActor extends LWActor {
    private final static Logger logger = LoggerFactory.getLogger(TCPClientActor.class);

    private static final String END_OF_SUSPEND = "end-of-suspend";
    private static final String RECONNECT_NOW = "reconnect-now";

    protected Optional<LWActorRef> connection = Optional.absent();
    protected Optional<TCP.Connect> connecting = Optional.absent();
    protected Optional<TCP.Close> closing = Optional.absent();

    abstract protected InetSocketAddress getAddress();

    private int retryCount;
    private long suspendTime;

    private int currentRetry = 0;

    /**
     * tcp connection 을 항상 유지하는 것을 자동으로 할지 결정한다.
     * false 일 경우 prestart 에서 connect 하거나 연결이 끊어질 경우 재시도도 하지 않는다.
     * @return
     */
    protected boolean autoConnection() {
        return true;
    }
    protected long retryAfter() {
        return 0;
    }
    protected Tuple2<Integer, Long> suspendAfter() {
        return new Tuple2(10, 5000l);
    }

    @Override
    final protected void preStart() {
        if (autoConnection())
            connect();
        Tuple2<Integer, Long> s = suspendAfter();
        retryCount = s._1;
        suspendTime = s._2;
        doPreStart();
    }

    /**
     * close current connection (if any)
     */
    final protected void close() {
        if (connection.isPresent()) {
            if (closing.isAbsent()) {
                closing = Optional.of(new TCP.Close());
                connection.get().send(closing.get());
            } else {
                logger.warn("close already requested");
            }
        }
    }

    protected void connect() {
        if (connecting.isPresent()) {
            logger.debug("connection already requested");
        } else if (connection.isAbsent()) {
            LWActorRef tcp = TCP.getManager();
            connecting = Optional.of(new TCP.Connect(getAddress()));
            tcp.send(connecting.get());
        }
    }

    protected boolean isConnection() {
        return connecting.isPresent();
    }

    private void connected() {
        if (closing.isPresent()) {
            logger.error("Internal Error: connection state corrupted.");
            closing = Optional.absent();
        }
        connection = Optional.of(sender.get());
        connecting = Optional.absent();
        sender.get().send(new TCP.Register(self));
        onConnected();
    }

    private void closed() {
        if (connection.isAbsent())
            return;
        LWActorRef closedConn = sender.get();
        if (closedConn != connection.get()) {
            logger.debug("received unexpected close event");
            return;
        }
        connection = Optional.absent();
        closing = Optional.absent();
        onClosed();
    }

    private void retry() {
        if (connection.isAbsent() && autoConnection()) {
            if (currentRetry < retryCount) {
                currentRetry ++;
                if (retryAfter() == 0)
                    connect();
                else if (retryAfter() > 0) {
                    self.getSystem().scheduleTimer(self, RECONNECT_NOW, retryAfter(), TimeUnit.MILLISECONDS);
                }
            } else {
                currentRetry = 0;
                self.getSystem().scheduleTimer(self, END_OF_SUSPEND, suspendTime, TimeUnit.MILLISECONDS);
            }
        }
    }

    @Override
    final protected boolean receive(Object message) {
        if (message == END_OF_SUSPEND) {
            connect();
            return true;
        } if (message == RECONNECT_NOW) {
            connect();
            return true;
        } else if (message instanceof TCP.CommandFailed) {
            if (connecting.isPresent()) {
                // if TCP.Connect command is current connection trial, start over.
                if (((TCP.CommandFailed) message).command == connecting.get()) {
                    connecting = Optional.absent();
                    retry();
                }
            } else if (connection.isPresent()) {
                // close connection on any error.
                logger.warn("Closing connection {} due to error", getAddress(), ((TCP.CommandFailed) message).cause);
                close();
            }
        } else if (message instanceof TCP.Closed) {
            closed();
            retry();
        } else if (message instanceof TCP.Connected) {
            connected();
        }

        return doReceive(message);
    }

    abstract protected boolean doReceive(Object message);
    protected void doPreStart() {};
    protected void onConnected() {}
    protected void onClosed() {}
}
