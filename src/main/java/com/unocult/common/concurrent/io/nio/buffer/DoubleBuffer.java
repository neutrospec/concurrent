package com.unocult.common.concurrent.io.nio.buffer;

import java.nio.ByteBuffer;

public class DoubleBuffer {
    private ByteBuffer buffer;
    private int writePosition = 0;
    private int readPosition = 0;
    private enum Mode {
        None, Read, Write
    }
    private Mode mode = Mode.None;

    public DoubleBuffer(ByteBuffer buffer) {
        this.buffer = buffer;
        mode = Mode.None;
    }

    public void clear() {
        buffer.clear();
        mode = Mode.None;
    }

    public void beginWrite() {
        ensureNotInTransaction();
        buffer.position(writePosition);
        buffer.limit(buffer.capacity());
        mode = Mode.Write;
    }
    public void beginRead() {
        ensureNotInTransaction();
        buffer.position(readPosition);
        buffer.limit(writePosition);
        mode = Mode.Read;
    }
    public void end() {
        switch (mode) {
            case None:
                throw new IllegalStateException("nothing to end");
            case Write:
                writePosition = buffer.position();
                break;
            case Read:
                readPosition = buffer.position();
                break;
        }
        if (readPosition == writePosition) {
            readPosition = 0;
            writePosition = 0;
        }
        mode = Mode.None;
    }
    public ByteBuffer readBuffer() {
        if (mode != Mode.Read)
            throw new IllegalStateException("not in read mode");
        return buffer;
    }
    public ByteBuffer writeBuffer() {
        if (mode != Mode.Write)
            throw new IllegalStateException("not in write mode");
        return buffer;
    }
    public int readableSize() {
        return writePosition - readPosition;
    }
    public int writableSize() {
        return buffer.capacity() - writePosition;
    }
    public void optimize(float ratio) {
        assert ratio >=0 && ratio <= 1;
        boolean compact = buffer.capacity() * ratio < readPosition;
        if (compact) {
            buffer.position(readPosition);
            buffer.limit(writePosition);
            buffer.compact();
            readPosition = 0;
            writePosition = buffer.position();
        }
    }
    public ByteBuffer rawBuffer() {return buffer;}
    private void ensureNotInTransaction() {
        if (mode != Mode.None)
            throw new IllegalStateException("not in write mode");
    }
}
