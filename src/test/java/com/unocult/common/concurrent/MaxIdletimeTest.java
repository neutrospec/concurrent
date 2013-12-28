package com.unocult.common.concurrent;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import static org.junit.Assert.*;

public class MaxIdletimeTest {
    private static final Logger logger = LoggerFactory.getLogger(MaxIdletimeTest.class);
    private static SynchronousQueue<Object> q = new SynchronousQueue<Object>();
    private static LWActorRef server;

    @Test
    public void testSetMaxIdleTimeout() throws Exception {
        ConcurrentSystem system = new ConcurrentSystem();
        system.start();

        server = system.actorOf(MyActor.class);
        for (int i = 0; i < 10; i++) {
            Object m = q.take();
            if (m instanceof String) {
                logger.info("Received: {}", m);
            } else {
                assertFalse(m instanceof Exception);
            }
        }

        server.stop();
        system.shutdown();
    }

    public static class MyActor extends LWActor {
        @Override
        protected void preStart() {
            setMaximumIdleTime(1, TimeUnit.SECONDS);
        }

        @Override
        protected boolean receive(Object message) {
            try {
                if (message == ConcurrentSystem.IdleTimeout) {
                    q.put("IdleTimeout");
                }
            } catch (Exception e) {
                q.offer(e);
            }
            return true;
        }
    }
}
