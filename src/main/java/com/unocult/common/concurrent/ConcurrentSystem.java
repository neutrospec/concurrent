package com.unocult.common.concurrent;

import com.unocult.common.base.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class ConcurrentSystem {
    protected static final String PreStart = "prestart";
    protected static final String RequestTimeout = "request-timeout";

    private static final Logger logger = LoggerFactory.getLogger(ConcurrentSystem.class);

    private ScheduledExecutorService scheduler;
    private BlockingQueue<MailBox> queues = new LinkedBlockingQueue<MailBox>();

    private volatile boolean running = false;
    private int batchSize = 100;
    private int threadPoolSize = 64;
    private Thread runner;

    void scheduleQueue(MailBox q) {
        if (q.isValidRef())
            queues.add(q);
        else {
            logger.error("dead actor scheduled: " + q);
        }
    }

    public static ConcurrentSystem getConcurrentSystem() {
        if (LWActorRef.callingActorRef.get() == null)
            return null;
        return LWActorRef.callingActorRef.get().getSystem();
    }
    ScheduledFuture scheduleTimer(final LWActorRef actor, final Object msg, final long delay, final TimeUnit unit) {
        return scheduler.schedule(new Runnable() {
            @Override
            public void run() {
                actor.mailBox.put(msg, Optional.<LWActorRef>absent());
            }
        }, delay, unit);
    }

    synchronized public void start() {
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

    public <T extends LWActor> LWActorRef actorOf(Class<T> klass) {
        LWActorRef actor = null;
        try {
            ActorProperty props = new ActorProperty(klass);
            MailBox mbox = new MailBox(props, this);
            mbox.put(PreStart, Optional.<LWActorRef>absent());
            actor = mbox.getOwner().get();
        } catch (Exception e) {
            logger.error("Actor instantiation error", e);
        }
        return actor;
    }

    public <T extends LWActor> LWActorRef actorOf(Class<T> klass, Object... args) {
        LWActorRef actor = null;
        try {
            ActorProperty props = new ActorProperty(klass, args);
            MailBox mbox = new MailBox(props, this);
            mbox.put(PreStart, Optional.<LWActorRef>absent());
            actor = mbox.getOwner().get();
        } catch (Exception e) {
            logger.error("Actor instantiation error", e);
        }
        return actor;
    }

    public synchronized void shutdown() {
        running = false;
        runner.interrupt();
        scheduler.shutdown();
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
}
