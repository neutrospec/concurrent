package com.unocult.common.concurrent;

import com.unocult.common.base.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.util.UUID;

public class ActorProperty {
    private static final Logger logger = LoggerFactory.getLogger(ActorProperty.class);
    protected String name;
    private final UUID id;
    private Object[] args = null;
    private Class<? extends LWActor> klass;
    private Optional<LWActorRef> parent = Optional.absent();

    public ActorProperty(String name, Class<? extends LWActor> klass) {
        this(name, klass, null);
    }

    public ActorProperty(String name, Class<? extends LWActor> klass, Object... args) {
        id = UUID.randomUUID();
        if (name == null)
            name = id.toString();
        this.name = name;
        this.klass = klass;
        this.args = args;
        parent = LWActorRef.findSender();
    }

    public Optional<LWActorRef> getParent() {
        return parent;
    }

    public void setParent(Optional<LWActorRef> parent) {
        this.parent = parent;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public UUID getId() {
        return id;
    }

    LWActor newInstance() {
        try {
            LWActor actorImpl = null;
            if (args != null) {
                Constructor[] cons = klass.getConstructors();
                for (Constructor c: cons) {
                    if (c.getParameterTypes().length == args.length)
                        actorImpl = (LWActor) c.newInstance(args);
                }
            } else
                actorImpl = klass.newInstance();
            actorImpl.setParent(getParent());
            return actorImpl;
        } catch (Throwable t) {
            logger.error("Actor property error: ", t);
        }

        // FIXME: what to do?
        return null;
    }
}
