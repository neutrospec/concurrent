package com.unocult.common.concurrent;

import org.apache.log4j.Logger;
import org.junit.Test;
import static org.junit.Assert.*;

import java.util.concurrent.SynchronousQueue;

public class SendRequestTest {
    private static final Logger logger = Logger.getLogger(SendRequestTest.class);
    private static SynchronousQueue<String> q = new SynchronousQueue<String>();
    private static LWActorRef client;
    private static LWActorRef server;

    @Test
    public void testSendReply() throws Exception {
        ConcurrentSystem system = new ConcurrentSystem();
        system.start();

        server = system.actorOf(Server.class);
        client = system.actorOf(Client.class);

        client.send("hi");
        String reply = q.take();
        assertEquals("there", reply);
        logger.info("reply: " + reply);
    }

    @Test
    public void testSendReplyTimeout() throws Exception {
        ConcurrentSystem system = new ConcurrentSystem();
        system.start();

        server = system.actorOf(Server.class);
        client = system.actorOf(Client.class);

        client.send("do not reply");
        String reply = q.take();
        assertEquals("timeout", reply);
        logger.info("reply: " + reply);
    }

    static class Client extends LWActor {
        @Override
        protected boolean receive(Object message) {
            logger.info("client received message: " + message);
            logger.info("sender: " + sender.isPresent());
            sendRequest(server, message, new ReplyHandler() {
                @Override
                public boolean isValidReply(Object reply) {
                    logger.info("reply validataion: " + reply);
                    return reply instanceof String;
                }

                @Override
                public void processReply(Object reply) {
                    try {
                        q.put((String) reply);
                    } catch (Exception e) {
                        logger.error("queue error", e);
                    }
                }

                @Override
                public void processTimeout() {
                    logger.info("timeout occurred!!");
                    try {
                        q.put("timeout");
                    } catch (Exception e) {
                        logger.error(e);
                    }
                }

                @Override
                public long timeout() {
                    return 3000;
                }
            });
            return true;
        }
    }

    static class Server extends LWActor {
        @Override
        protected boolean receive(Object message) {
            logger.info("sender: " + sender.isPresent());
            logger.info("server received message: " + message);
            if ("do not reply".equals(message)) {
                logger.info("we have to make timeout");
            } else
                sendReply("there");

            return true;
        }
    }
}
