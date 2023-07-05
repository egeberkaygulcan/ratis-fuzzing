package org.apache.ratis.server.impl.comm;

import java.util.HashMap;
import java.util.Vector;
import java.util.concurrent.locks.ReentrantLock;

public class Timer {
    private HashMap<String, Timeout> timeouts;
    private Vector<Timeout> ready;
    private final ReentrantLock lock = new ReentrantLock();

    public Timer() {
        this.timeouts = new HashMap<String, Timeout>();
        this.ready = new Vector<Timeout>();
    }

    public boolean addTimeout(Timeout t) {
        boolean result = false;
        lock.lock();
        try {
            if (!timeouts.containsKey(t.key())) {
                result = true;
                timeouts.put(t.key(), t);
            }
        } finally {
             lock.unlock();
        }
        return result;
    }

    public void fireTimeout(String key) {
        lock.lock();
        try {
            if(timeouts.containsKey(key)) {
                Timeout timeout = timeouts.get(key);
                ready.add(timeout);
                timeouts.remove(key);
            }
        } finally {
            lock.unlock();
        }
    }

    public Vector<Timeout> getReady() {
        if(ready.isEmpty()){
            return new Vector<Timeout>();
        }
        Vector<Timeout> result = new Vector<Timeout>();
        result.addAll(ready);
        ready.clear();
        return result;
    }
}
