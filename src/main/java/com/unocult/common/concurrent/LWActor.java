package com.unocult.common.concurrent;

import com.unocult.common.base.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public abstract class LWActor {
    private static final Logger logger = LoggerFactory.getLogger(LWActor.class);
    private MailBox mailBox;
    enum State {
        PreStart, Started, Stop
    }
    private State state = State.PreStart;

    protected LWActorRef self;
    protected Optional<LWActorRef> sender = Optional.absent();
    protected Optional<LWActorRef> parent = Optional.absent();

    protected long requestId = 0;
    protected Optional<LWActor.ReplyHandler> replHandler = Optional.absent();
    protected Optional<ScheduledFuture> timeoutTimer = Optional.absent();

    protected int errorCount = 0;
    protected Optional<Throwable> lastThrowable = Optional.absent();

    protected abstract boolean receive(Object message);

    protected void preStart() {};
    protected void postStop() {};

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

    void setMailBox(MailBox mbox) {
        this.mailBox = mbox;
    }

    public final void sendRequest(LWActorRef dest, Object message, LWActor.ReplyHandler handler) {
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
