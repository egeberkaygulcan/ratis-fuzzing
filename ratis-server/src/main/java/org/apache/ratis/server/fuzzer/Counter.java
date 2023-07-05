package org.apache.ratis.fuzzing;

import java.util.concurrent.locks.ReentrantLock;

public class Counter {
    private long value = 0;

    private final ReentrantLock lock = new ReentrantLock();

    public Counter() {}

    public long getValue() {
        long result = 0;
        lock.lock();
        result = value;
        lock.unlock();
        return result;
    }

    public void incr() {
        lock.lock();
        value = value + 1;
        lock.unlock();
    }
}
