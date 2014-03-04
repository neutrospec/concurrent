package com.unocult.common.concurrent;

import com.unocult.common.annotations.GuardedBy;
import com.unocult.common.base.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ConcurrentSystem {
    protected static final String PreStart = "prestart";
    protected static final String RequestTimeout = "request-timeout";

    private static final Logger logger = LoggerFactory.getLogger(ConcurrentSystem.class);

    private ScheduledExecutorService scheduler;
    private BlockingQueue<MailBox> queues = new LinkedBlockingQueue<MailBox>();

    private volatile boolean running = false;
    private int batchSize = 100;
    private int threadPoolSize = 256;
    private Thread runner;

    private Lock lock = new ReentrantLock();

    @GuardedBy("lock")
    private Map<String, LWActorRef> actors = new Hashtable<String, LWActorRef>();
    @GuardedBy("lock")
    private List<LWActorRef> rootActors = new LinkedList<LWActorRef>();

    void scheduleQueue(MailBox q) {
        if (q.isValidRef())
            queues.add(q);
        else {
            logger.error("dead actor scheduled: " + q);
        }
    }

    public Optional<LWActorRef> findActorByName(String name) {
        lock.lock();
        try {
            LWActorRef ref = actors.get(name);
            if (ref != null)
                return Optional.of(ref);
            return Optional.absent();
        } finally {
            lock.unlock();
        }
    }

    public static ConcurrentSystem getConcurrentSystem() {
        if (LWActorRef.callingActorRef.get() == null)
            return null;
        return LWActorRef.callingActorRef.get().getSystem();
    }

    public ScheduledFuture scheduleTimer(final LWActorRef actor, final Object msg, final long delay, final TimeUnit unit) {
        return scheduler.schedule(new Runnable() {
            @Override
            public void run() {
                actor.mailBox.put(msg, Optional.<LWActorRef>absent());
            }
        }, delay, unit);
    }

    synchronized public void start() {
        if (running)
            return;
        logger.info("Starting ConcurrentSystem...");
        scheduler = Executors.newScheduledThreadPool(threadPoolSize);
        running = true;
        runner = new Thread(new Runnable() {
            @Override
            public void run() {
                loop();
            }
        });
        runner.start();
    }

    public <T extends LWActor> LWActorRef _actorOf(String name, Class<T> klass, Object[] args) {
        LWActorRef actor = null;
        try {
            ActorProperty props = new ActorProperty(name, klass, args);
            MailBox mbox = new MailBox(props, this);
            actor = mbox.getOwner().get();
            bindActorToUniqueName(actor, props);
            mbox.put(PreStart, Optional.<LWActorRef>absent());
        } catch (Exception e) {
            logger.error("Actor instantiation error", e);
        }
        return actor;
    }

    private void bindActorToUniqueName(LWActorRef actor, ActorProperty props) {
        lock.lock();
        try {
            if (actors.containsKey(props.getName())) {
                props.setName(props.getId().toString());
                logger.warn("'{}' is already in use. '{}' will be actors name");
            }
            actors.put(props.getName(), actor);
            if (actor.parent.isAbsent()) {
                rootActors.add(actor);
            }
        } finally {
            lock.unlock();
        }
    }

    public <T extends LWActor> LWActorRef actorOf(String name, Class<T> klass, Object... args) {
        return _actorOf(name, klass, args);
    }

    public <T extends LWActor> LWActorRef actorOf(Class<T> klass) {
        return _actorOf(null, klass, null);
    }

    public <T extends LWActor> LWActorRef actorOf(Class<T> klass, Object... args) {
        return _actorOf(null, klass, args);
    }

    protected void releaseActor(LWActorRef actor) {
        lock.lock();
        try {
            String name = actor.mailBox.props.getName();
            LWActorRef ref = actors.get(name);
            if (ref != null && actor == ref) {
                actors.remove(name);
                rootActors.remove(actor);
            }
        } finally {
            lock.unlock();
        }
    }

    public synchronized void shutdown() {
        if (running) {
            running = false;
            runner.interrupt();
            scheduler.shutdown();
            logger.info("ConcurrentSystem terminated.");
        }
    }

    public void deadLetter(LWActorRef actor, Object letter) {
        logger.warn("Dead letter from {}: {}", actor, letter);
    }

    private void loop() {
        while (running) {
            try {
                final MailBox mbox = queues.take();
                scheduler.execute(new Runnable() {
                    @Override
                    public void run() {
                        int remain = 0;
                        try {
                            remain = mbox.process(batchSize);
                        } catch (Throwable t) {
                            logger.error("Actor raised an error", t);
                        } finally {
                            if (remain > 0)
                                scheduleQueue(mbox);
                        }
                    }
                });
            } catch (InterruptedException e) {
                if (running)
                    logger.error("Actor system run loop error", e);
            } catch (Throwable t) {
                logger.error("Actor system run loop error", t);
            }
        }
    }

    static class Timeout {
        public final long id;
        Timeout(long id) {
            this.id = id;
        }
    }
    public static class Terminated {
        public final LWActorRef actor;

        public Terminated(LWActorRef actor) {
            this.actor = actor;
        }
    }
    public static final Object PosonPill = new Object();
    public static final Object Watch = new Object();
    public static final Object IdleTimeout = new Object();
}
