package com.unocult.common.concurrent;

import org.apache.log4j.Logger;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;

public class ConcurrentSystemTest {
    static private final Logger logger = Logger.getLogger(ConcurrentSystemTest.class);
    private static CountDownLatch latch = null;
    private static LWActorRef another = null;
    int CALL_COUNT = 10000000;

    @Test
    public void testArgActor() throws Exception {
        ConcurrentSystem system = new ConcurrentSystem();
        latch = new CountDownLatch(CALL_COUNT);
        system.start();

        long start = System.currentTimeMillis();
        LWActorRef actor = system.actorOf(MyArgActor.class, "MyName");
        for (int i = 0; i < CALL_COUNT; i++)
            actor.send("MyName");
        latch.await();
        long end = System.currentTimeMillis();
        logger.info("ArgActor: " + CALL_COUNT + " in " + (end - start));

        system.shutdown();
    }

    @Test
    public void testActor() throws Exception {
        ConcurrentSystem system = new ConcurrentSystem();
        latch = new CountDownLatch(CALL_COUNT);
        system.start();

        long start = System.currentTimeMillis();
        LWActorRef actor = system.actorOf(AnotherActor.class);
        for (int i = 0; i < CALL_COUNT; i++)
            actor.send("down");
        latch.await();
        long end = System.currentTimeMillis();
        logger.info("Actor: " + CALL_COUNT + " in " + (end - start));
        system.shutdown();
    }

    @Test
    public void testActorChain() throws Exception {
        ConcurrentSystem system = new ConcurrentSystem();
        latch = new CountDownLatch(CALL_COUNT);
        system.start();
        long start = System.currentTimeMillis();
        LWActorRef actor = system.actorOf(MyActor.class);
        another = system.actorOf(AnotherActor.class);

        for (int i = 0; i < CALL_COUNT; i++)
            actor.send("down");
        latch.await();
        long end = System.currentTimeMillis();
        logger.info("ActorChain: " + CALL_COUNT + " in " + (end - start));
        system.shutdown();
    }

    @Test
    public void testControl() throws Exception {
        ConcurrentSystem system = new ConcurrentSystem();
        latch = new CountDownLatch(CALL_COUNT);
        system.start();
        long start = System.currentTimeMillis();
        for (int i = 0; i < CALL_COUNT; i++)
            latch.countDown();
        latch.await();
        long end = System.currentTimeMillis();
        logger.info("Control: " + CALL_COUNT + " in " + (end - start));
    }

    static class MyActor extends LWActor {
        @Override
        protected void preStart() {
            logger.info("MyActor pre-start");
        }

        @Override
        protected boolean receive(Object message) {
            forward(another, message);
            return true;
        }
    }

    static class AnotherActor extends LWActor {
        @Override
        protected void preStart() {
            logger.info("AnotherActor pre-start");
        }
        @Override
        protected boolean receive(Object message) {
            latch.countDown();
            return true;
        }
    }

    static class MyArgActor extends LWActor {
        private String name;

        public MyArgActor(String name) {
            this.name = name;
        }

        @Override
        protected boolean receive(Object message) {
            if (name.equals(message))
                latch.countDown();
            return true;
        }
    }
}

