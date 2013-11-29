package com.unocult.common.concurrent.io.message;

public class SocketProblem {
    public final Throwable cause;

    public SocketProblem(Throwable cause) {
        this.cause = cause;
    }
}
