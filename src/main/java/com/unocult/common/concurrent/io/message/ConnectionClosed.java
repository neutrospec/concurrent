package com.unocult.common.concurrent.io.message;

import com.unocult.common.concurrent.io.nio.Connection;

public class ConnectionClosed {
    public final Connection connection;
    public ConnectionClosed(Connection connection) {
        this.connection = connection;
    }
}
