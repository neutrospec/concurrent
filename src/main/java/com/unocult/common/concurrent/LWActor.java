package com.unocult.common.concurrent;

import com.unocult.common.base.Optional;
import com.unocult.common.base.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public abstract class LWActor {
    private static final Logger logger = LoggerFactory.getLogger(LWActor.class);
    enum State {
        PreStart, Started, Stop
    }
    private State state = State.PreStart;

    private long lastActivity = 0;
    private Optional<Tuple2<Long, TimeUnit>> maximumIdleTimeout = Optional.absent();
    private Optional<ScheduledFuture> maximumIdleTimeoutFuture = Optional.absent();
    private long timeout = 0;

    protected LWActorRef self;
    protected Optional<LWActorRef> sender = Optional.absent();
    protected Optional<LWActorRef> parent = Optional.absent();

    protected long requestId = 0;
    protected Optional<ReplyHandler> replHandler = Optional.absent();
    protected Optional<ScheduledFuture> timeoutTimer = Optional.absent();
    protected int errorCount = 0;
    protected Optional<Throwable> lastThrowable = Optional.absent();

    protected abstract boolean receive(Object message);

    protected void preStart() {};
    protected void postStop() {};

    protected void setMaximumIdleTime(long time, TimeUnit unit) {
        lastActivity = System.currentTimeMillis();
        maximumIdleTimeout = Optional.of(new Tuple2<Long, TimeUnit>(time, unit));
        ScheduledFuture future = self.getSystem().scheduleTimer(self, ConcurrentSystem.IdleTimeout, time, unit);
        maximumIdleTimeoutFuture = Optional.of(future);
        timeout = unit.toMillis(time);
    }
    private void scheduleMaximumIdleTime() {
        if (maximumIdleTimeout.isPresent()) {
            long passed = System.currentTimeMillis() - lastActivity;
            long next = timeout;
            if (passed >= timeout) {
                updateLastActivityTime();
                receive_(ConcurrentSystem.IdleTimeout);
            } else if (passed > 0 && passed < timeout) {
                next = timeout - passed;
            }
            ScheduledFuture future = self.getSystem().scheduleTimer(self, ConcurrentSystem.IdleTimeout, next, TimeUnit.MILLISECONDS);
            maximumIdleTimeoutFuture = Optional.of(future);
        }
    }
    protected void cancelIdleTime() {
        if (maximumIdleTimeoutFuture.isPresent()) {
            maximumIdleTimeoutFuture.get().cancel(false);
            maximumIdleTimeoutFuture = Optional.absent();
        }
    }
    boolean processMessage(Object message, Optional<LWActorRef> sender) {
        this.sender = sender;
        boolean result = true;

        switch (state) {
            case PreStart:
                preStart();
                state = State.Started;
                if (message != ConcurrentSystem.PreStart)
                    result = receive_(message);
                break;
            case Started:
                if (message == ConcurrentSystem.IdleTimeout) {
                    scheduleMaximumIdleTime();
                    break;
                }
                updateLastActivityTime();
                // ignore unrelated message
                if (message == ConcurrentSystem.PreStart) {
                    logger.warn("Prestart messaged received more than once");
                    break;
                }
                if (replHandler.isPresent()) {
                    if (requestTimeout(message))
                        return true;
                    if (replHandler.get().isValidReply(message)) {
                        replHandler.get().processReply(message);
                        result = true;
                    } else
                        result = false;
                } else
                    result = receive_(message);
                break;
            case Stop:
                logger.warn("Stopped actor cannot handle message: " + message);
                break;
        }
        return result;
    }

    private boolean requestTimeout(Object message) {
        // process reply
        if (message instanceof ConcurrentSystem.Timeout) {
            if (((ConcurrentSystem.Timeout) message).id != requestId) {
                logger.debug("Timeout id mismatch: " + ((ConcurrentSystem.Timeout) message).id);
                return true;
            } else {
                timeoutTimer = Optional.absent();
                replHandler.get().processTimeout();
                replHandler = Optional.absent();
                return true;
            }
        }
        return false;
    }

    private boolean receive_(Object message) {
        boolean result = true;
        try {
            result = receive(message);
        } catch (Throwable t) {
            if (parent.isPresent()) {

            }
        }
        return result;
    }

    protected final boolean sendReply(Object reply) {
        if (sender.isAbsent()) {
            logger.debug("reply error: no sender");
            return false;
        }
        sender.get().send(reply);
        return true;
    }

    public Optional<LWActorRef> getParent() {
        return parent;
    }

    public void setParent(Optional<LWActorRef> parent) {
        this.parent = parent;
    }

    public final void sendRequest(LWActorRef dest, Object message, ReplyHandler handler) {
        requestId ++;

        ScheduledFuture future = self.getSystem().scheduleTimer(self, new ConcurrentSystem.Timeout(requestId), handler.timeout(), TimeUnit.MILLISECONDS);
        timeoutTimer = Optional.of(future);
        replHandler = Optional.of(handler);

        dest.send(message);
    }

    protected final void forward(LWActorRef dest, Object message) {
        dest.mailBox.put(message, sender);
    }

    public interface ReplyHandler {
        boolean isValidReply(Object reply);
        void processReply(Object reply);
        void processTimeout();
        long timeout();
    }

    private void updateLastActivityTime() {
        lastActivity = System.currentTimeMillis();
    }
    protected Optional<LWActorRef> getSender() {
        return sender;
    }

    protected void setSender(Optional<LWActorRef> sender) {
        this.sender = sender;
    }

    public LWActorRef getSelf() {
        return self;
    }

    protected ConcurrentSystem getSystem() {
        return self.getSystem();
    }

    public void setSelf(LWActorRef self) {
        this.self = self;
    }
}
