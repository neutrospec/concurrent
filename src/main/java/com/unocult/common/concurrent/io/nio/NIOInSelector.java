package com.unocult.common.concurrent.io.nio;

import java.io.IOException;
import java.nio.channels.SelectionKey;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NIOInSelector extends AbstractSelector {
	private static Logger log = LoggerFactory.getLogger(NIOInSelector.class);
	public NIOInSelector(ConnectionManager connectionManager) {
		this.connectionManager = connectionManager;
		selector.wakeup();
	}

    protected void process(SelectionKey key) throws IOException {
        Connection connection = (Connection) key.attachment();
        if (connection == null) {
            log.error("invalid key attachment!!");
            key.channel().close();
            return;
        }
        int readyOps = key.readyOps();
        if ((readyOps & SelectionKey.OP_READ) != 0 || readyOps == 0)
            connection.doRead();
        if ((readyOps & SelectionKey.OP_WRITE) != 0)
            connection.doWrite();
    }
}
