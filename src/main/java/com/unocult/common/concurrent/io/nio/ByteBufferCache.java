package com.unocult.common.concurrent.io.nio;

import java.nio.ByteBuffer;
import java.util.LinkedList;

public class ByteBufferCache {
    private LinkedList<ByteBuffer> buffers = new LinkedList<ByteBuffer>();
    private int limit;
    public ByteBufferCache(int limit) {
        this.limit = limit;
    }
    public synchronized ByteBuffer getDirectByteBuffer(int capacity) {
        ByteBuffer buffer = null;
        while (buffers.size() > 0) {
            buffer = buffers.removeFirst();
            if (buffer.capacity() >= capacity)
                return buffer;
        }
        return ByteBuffer.allocateDirect(capacity);
    }

    public synchronized void releaseDirectByteBuffer(ByteBuffer byteBuffer) {
        if (byteBuffer == null)
            return;
        if (!byteBuffer.isDirect())
            return;
        if (limit <= buffers.size())
            return;
        buffers.addLast(byteBuffer);
    }
}
