package com.unocult.common.base;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

public class SingleList<T> implements List<T> {
    private final T item;

    public SingleList(T item) {
        this.item = item;
    }
    @Override
    public int size() {
        return 1;
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public boolean contains(Object o) {
        if (o == null)
            return false;
        if (o == item)
            return true;
        return item.equals(o);
    }

    @Override
    public Iterator<T> iterator() {
        Iterator<T> iter = new Iterator<T>() {
            boolean read = false;
            @Override
            public boolean hasNext() {
                return !read;
            }

            @Override
            public T next() {
                read = true;
                return item;
            }

            @Override
            public void remove() {
            }
        };
        return iter;
    }

    @Override
    public Object[] toArray() {
        Object [] array =  new Object[1];
        array[0] = item;
        return array;
    }

    @Override
    public <T1> T1[] toArray(T1[] a) {
        a[0] = (T1) item;
        return a;
    }

    @Override
    public boolean add(T t) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean remove(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        if (c.size() != 1)
            return false;

        return c.contains(item);
    }

    @Override
    public boolean addAll(Collection<? extends T> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean addAll(int index, Collection<? extends T> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof Collection) {
            Collection c = (Collection) o;
            if (c.size() != 1)
                return false;
            return c.contains(item);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return item.hashCode() + 1;
    }

    @Override
    public T get(int index) {
        if (index == 0)
            return item;
        throw new IndexOutOfBoundsException("" + index + " is not 0");
    }

    @Override
    public T set(int index, T element) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void add(int index, T element) {
        throw new UnsupportedOperationException();
    }

    @Override
    public T remove(int index) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int indexOf(Object o) {
        if (o == item)
            return 0;
        return -1;
    }

    @Override
    public int lastIndexOf(Object o) {
        return 0;
    }

    @Override
    public ListIterator<T> listIterator() {
        return null;
    }

    @Override
    public ListIterator<T> listIterator(int index) {
        return null;
    }

    @Override
    public List<T> subList(int fromIndex, int toIndex) {
        return null;
    }
}
