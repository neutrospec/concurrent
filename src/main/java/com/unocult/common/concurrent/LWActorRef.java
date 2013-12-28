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

    private final Lock childrenLock = new ReentrantLock();
    private final Lock watcherLock = new ReentrantLock();

    @GuardedBy("childrenLock")
    private List<LWActorRef> children = new LinkedList<LWActorRef>();

    @GuardedBy("watcherLock")
    private final List<LWActorRef> watchers = new LinkedList<LWActorRef>();

    protected Optional<LWActorRef> parent = Optional.absent();

    // represent current operating LWActorRef
    protected static ThreadLocal<LWActorRef> callingActorRef = new ThreadLocal<LWActorRef>();

    LWActorRef(ConcurrentSystem system, MailBox mailBox, ActorProperty props) {
        this.system = system;
        this.mailBox = mailBox;
        this.props = props;
        this.parent = LWActorRef.findSender();
        allocateActorImpl();
        if (parent.isPresent())
            parent.get().addChildren(this);
    }

    boolean processMessage(Object message, Optional<LWActorRef> sender) {
        boolean result = true;
        try {
            callingActorRef.set(this);
            // error processing point!!!
            actorImpl.get().setSender(sender);
            result = actorImpl.get().processMessage(message, sender);
            callingActorRef.remove();
        } catch (Exception e) {
            logger.error("ACTOR SYSTEM", e);
        }
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

    protected void addChildren(LWActorRef child) {
        childrenLock.lock();
        try {
            if (!children.contains(child))
                children.add(child);
            else
                logger.error("child actor register error: duplicated.");
        } finally {
            childrenLock.unlock();
        }
    }

    protected void removeChild(LWActorRef child) {
        childrenLock.lock();
        try {
            if (!children.remove(child))
                logger.error("unregistered child error");
        } finally {
            childrenLock.unlock();
        }
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

    protected void shutdown() {
        sendTerminateEventToWatchers();
        callDoPostOnActorImpl();
        shutdownAllChildActors();
        if (parent.isPresent()) {
            parent.get().removeChild(this);
        }
    }

    private void callDoPostOnActorImpl() {
        // clean up!!!!
        if (actorImpl.isPresent()) {
            actorImpl.get().cancelIdleTime();
            actorImpl.get().postStop();
        }
    }

    private void shutdownAllChildActors() {
        childrenLock.lock();
        try {
            for (LWActorRef child: children) {
                child.stop();
            }
        } finally {
            childrenLock.unlock();
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
