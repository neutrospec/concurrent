package com.unocult.common.concurrent;

import com.unocult.common.annotations.GuardedBy;
import com.unocult.common.base.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

class MailBox {
    private static final Logger logger = LoggerFactory.getLogger(MailBox.class);
    final ConcurrentSystem system;
    private int bound = 10000;

    enum State {
        Empty, Scheduled, Running, Stop
    }
    private int offset = 0;

    private final Lock stateLock = new ReentrantLock();
    private final Condition consumedCond = stateLock.newCondition();

    @GuardedBy("stateLock")
    private State mailBoxState = State.Empty;
    @GuardedBy("stateLock")
    private final LinkedList<Message> mailBox = new LinkedList<Message>();

    private Optional<LWActorRef> owner;
    protected final ActorProperty props;

    public MailBox(ActorProperty props, ConcurrentSystem system) {
        this.system = system;
        this.props = props;
        createActorRef();
    }

    protected void createActorRef() {
        LWActorRef actorRef = new LWActorRef(system, this, props);
        owner = Optional.of(actorRef);
    }

    @GuardedBy("stateLock")
    protected void add(Message msg) {
        while (mailBoxState != State.Stop && mailBox.size() >= bound) {
            try {consumedCond.await();} catch (InterruptedException e) {}
        }
        if (mailBoxState != State.Stop)
            mailBox.add(msg);
    }

    protected boolean isValidRef() {
        stateLock.lock();
        try {
            return mailBoxState != State.Stop;
        } finally {
            stateLock.unlock();
        }
    }

    public final void put(Object message, Optional<LWActorRef> sender) {
        Message msg = new Message(message, sender);
        boolean scheduleMe = false;
        stateLock.lock();
        try {
            add(msg);
            if (mailBoxState == State.Stop) {
                logger.warn("dead actor received message");
                return;
            }
            switch (mailBoxState) {
                case Running:
                    mailBoxState = State.Scheduled;
                    break;
                case Scheduled:
                    // no need to change
                    break;
                case Empty:
                    mailBoxState = State.Scheduled;
                    scheduleMe = true;
                    break;
                case Stop:
                    logger.error("dead actor does not accept message! " + this);
                    break;
                default:
                    // error?
                    break;
            }
        } catch (Exception e) {
            logger.error("ACTOR SYSTEM", e);
        } finally {
            stateLock.unlock();
        }
        if (scheduleMe) {
            system.scheduleQueue(this);
        }
    }

    int process(int batchSize) {
        Message head = null;
        Optional<LWActorRef> sender;
        stateLock.lock();
        try {
            if (owner.isAbsent())
                offset = mailBox.size();
            if (mailBox.size() <= offset) {
                mailBoxState = State.Empty;
                return 0;
            }
            mailBoxState = State.Running;
            head = mailBox.get(offset);
        } finally {
            stateLock.unlock();
        }

        // this 'for loop' must be a single threaded loop!!!
        // if mailbox had been scheduled twice, it will be disaster. BE CAREFUL
        for (int i = 0; i < batchSize; i++) {
            boolean result = true;
            Object msg = head.getMessage();
            if (msg == ConcurrentSystem.PosonPill) {
                logger.debug("actor received poison pill");
                stop();
                return 0;
            }
            result = owner.get().processMessage(head.getMessage(), head.getSender());

            if (result) {
                remove(offset);
                // receive 가 메시지를 처리하면 pending 되었던 메시지부터 다시 feeding 한다.
                offset = 0;
            } else {
                offset ++;
            }

            stateLock.lock();
            try {
                if (mailBox.size() <= offset)
                    break;
                else
                    head = mailBox.get(offset);
            } finally {
                stateLock.unlock();
            }
        }

        stateLock.lock();
        try {
            int remain = mailBox.size() - offset;
            if (remain == 0)
                mailBoxState = State.Empty;
            else
                mailBoxState = State.Scheduled;
            return remain;
        } finally {
            stateLock.unlock();
        }
    }

    private void remove(int index) {
        stateLock.lock();
        try {
            if (mailBox.size() >= bound)
                consumedCond.signalAll();
            mailBox.remove(offset);
        } finally {
            stateLock.unlock();
        }
    }

    void stop() {
        stateLock.lock();
        try {
            mailBoxState = State.Stop;
            if (getOwner().isPresent()) {
                getOwner().get().shutdown();
                system.releaseActor(getOwner().get());
            }
        } catch (Throwable throwable) {
            logger.warn("stopping mailbox error", throwable);
        } finally {
            stateLock.unlock();
        }
    }

    Optional<LWActorRef> getOwner() {
        return owner;
    }

    void setOwner(Optional<LWActorRef> owner) {
        this.owner = owner;
    }

    private class Message {
        Object message = null;
        private Optional<LWActorRef> sender = Optional.absent();

        private Optional<LWActorRef> getSender() {
            return sender;
        }

        private Object getMessage() {
            return message;
        }

        Message(Object message, Optional<LWActorRef> sender) {
            this.sender = sender;
            this.message = message;
        }
    }
}
