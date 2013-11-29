package com.unocult.common.base;

import java.io.Serializable;

public class Optional<T> implements Serializable  {
    private static final Optional ABSENT = new Optional(null);
    private T value;

    protected Optional(T value) {
        this.value = value;
    }

    public static <T> Optional<T> absent() {
        return ABSENT;
    }

    public static <T> Optional<T> of(T value) {
        return new Optional<T>(value);
    }

    public boolean isAbsent() {
        return (value == null);
    }

    public boolean isPresent() {
        return !isAbsent();
    }

    public T get() {
        if (isPresent())
            return this.value;
        throw new IllegalStateException("empty value");
    }

    public T orElse(T value) {
        if (isPresent())
            return this.value;
        return value;
    }

    public T orNull() {
        return orElse(null);
    }
}
