package com.unocult.common.concurrent;

import com.unocult.common.annotations.GuardedBy;
import com.unocult.common.base.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class LWActorRef {
    private static final Logger logger = LoggerFactory.getLogger(LWActorRef.class);

    private ConcurrentSystem system = null;
    protected MailBox mailBox;
    private ActorProperty props;

    protected Optional<LWActorRef> sender = Optional.absent();
    private Optional<LWActor> actorImpl = Optional.absent();

    private final Lock watcherLock = new ReentrantLock();

    @GuardedBy("watcherLock")
    private final List<LWActorRef> watchers = new LinkedList<LWActorRef>();

    // represent current operating LWActorRef
    protected static ThreadLocal<LWActorRef> callingActorRef = new ThreadLocal<LWActorRef>();

    LWActorRef(ConcurrentSystem system, MailBox mailBox, ActorProperty props) {
        this.system = system;
        this.mailBox = mailBox;
        this.props = props;

        allocateActorImpl();
    }

    boolean processMessage(Object message, Optional<LWActorRef> sender) {
        boolean result = true;
        callingActorRef.set(this);
        // error processing point!!!
        actorImpl.get().setSender(sender);
        result = actorImpl.get().processMessage(message, sender);
        callingActorRef.remove();
        return result;
    }

    public final void send(Object message) {
        mailBox.put(message, findSender());
    }

    public ConcurrentSystem getSystem() {
        return system;
    }

    private void watch_(LWActorRef watcher) {
        watcherLock.lock();
        try {
            watchers.add(watcher);
        } finally {
            watcherLock.unlock();
        }
    }
    private void unwatch_(LWActorRef watcher) {
        watcherLock.lock();
        try {
            watchers.remove(watcher);
        } finally {
            watcherLock.unlock();
        }
    }

    public void watch(LWActorRef target) {
        target.watch_(this);
    }

    public void unwatch(LWActorRef target) {
        target.unwatch_(this);
    }

    void setSystem(ConcurrentSystem system) {
        this.system = system;
    }

    protected void allocateActorImpl() {
        actorImpl = Optional.of(props.newInstance());
        actorImpl.get().setSelf(this);
    }

    protected static Optional<LWActorRef> findSender() {
        if (callingActorRef.get() == null)
            return Optional.absent();
        else
            return Optional.of(callingActorRef.get());
    }

    protected  void shutdown() {
        sendTerminateEventToWatchers();
        // clean up!!!!
        if (actorImpl.isPresent()) {
            actorImpl.get().postStop();
        }
    }

    private void sendTerminateEventToWatchers() {
        watcherLock.lock();
        try {
            for (LWActorRef watcher : watchers) {
                watcher.send(new ConcurrentSystem.Terminated(this));
                logger.debug("signal watcher: {}", watcher);
            }
        } finally {
            watcherLock.unlock();
        }
    }

    public void stop() {
        send(ConcurrentSystem.PosonPill);
    }
}
