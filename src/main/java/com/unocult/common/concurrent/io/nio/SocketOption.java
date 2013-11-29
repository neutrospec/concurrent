package com.unocult.common.concurrent.io.nio;

public class SocketOption {
    public static final int DEFAULT_WRITE_FRAGMENT_SIZE = 8 * 1024;
    private int sendBufferSize = 0;
    private int receiveBufferSize = 0;
    private boolean bundle = false;
    private int writeFragmentSize = DEFAULT_WRITE_FRAGMENT_SIZE;

    public int getSendBufferSize() {
        return sendBufferSize;
    }

    public void setSendBufferSize(int sendBufferSize) {
        this.sendBufferSize = sendBufferSize;
    }

    public int getReceiveBufferSize() {
        return receiveBufferSize;
    }

    public void setReceiveBufferSize(int receiveBufferSize) {
        this.receiveBufferSize = receiveBufferSize;
    }

    /**
     * @deprecated has no effect
     * @return
     */
    public boolean isBundle() {
        return bundle;
    }

    /**
     * @deprecated has no effect
     * @return
     */
    public void setBundle(boolean bundle) {
        this.bundle = bundle;
    }

    public int getConnectionBufferSize() {
        return writeFragmentSize;
    }

    public void setConnectionBufferSize(int writeFragmentSize) {
        this.writeFragmentSize = writeFragmentSize;
    }
}
