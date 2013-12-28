package com.unocult.common.concurrent.io;
import com.unocult.common.base.Optional;
import com.unocult.common.concurrent.ConcurrentSystem;
import com.unocult.common.concurrent.LWActor;
import com.unocult.common.concurrent.LWActorRef;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.SynchronousQueue;

import static org.junit.Assert.*;

public class TCPTest {
    private static final Logger logger = LoggerFactory.getLogger(TCPTest.class);
    private static final SynchronousQueue<String> queue = new SynchronousQueue<String>();
    @Test
    public void testConnectionToGoogle() throws Exception {
        ConcurrentSystem system = new ConcurrentSystem();
        system.start();

        system.actorOf(Client.class);

        String result = queue.take();
        logger.info("Test finished with: {}", result);

        system.shutdown();
    }

    public static class Client extends LWActor {
        Optional<LWActorRef> connection = Optional.absent();

        @Override
        protected void preStart() {
            logger.info("try to connect to google");
            TCP.getManager().send(new TCP.Connect(new InetSocketAddress("www.google.co.kr", 80)));
        }

        @Override
        protected void postStop() {
            try {
                queue.put("Quit");
            } catch (InterruptedException i) {
                logger.error("error", i);
            }
        }

        @Override
        protected boolean receive(Object message) {
            if (message instanceof TCP.Connected) {
                logger.info("connection established");
                connection = Optional.of(sender.get());
                connection.get().send(new TCP.Register(self));
                Get();
                logger.info("sent GET");
            } else if (message instanceof TCP.WriteAck) {
                logger.info("Get request OK");
            } else if (message instanceof TCP.Received) {
                logger.info("reading http reply");
                print((TCP.Received) message);
                self.stop();
//                connection.get().send(new TCP.Close());
            } else if (message instanceof TCP.Closed) {
                logger.info("http connection closed");
                connection = Optional.absent();
                self.stop();
            }

            return true;
        }

        private void Get() {
            String cmd = "GET / HTTP/1.1\nHOST: www.google.com\n\n";
            ByteBuffer data = ByteBuffer.wrap(cmd.getBytes());
            logger.debug("Write Buffer: {}", data);
            connection.get().send(new TCP.Write(data));
        }

        private void print(TCP.Received message) {
            byte[] array = new byte[message.data.remaining()];
            logger.debug("Received Bytebuffer: {}", message.data);
            int index = 0;
            logger.info("REPL: {}", new String(message.data.array()));
        }
    }
}
