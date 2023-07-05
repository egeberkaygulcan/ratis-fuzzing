package org.apache.ratis.server.fuzzer;

import java.util.HashMap;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.ratis.server.fuzzer.messages.Message;

public class FuzzerClient extends Thread{
    public static FuzzerClient fuzzerClient;
    HashMap<Long, Message> messageMap;
    Counter idCounter;

    public FuzzerClient() {
        this.messageMap = new HashMap<Long, Message>();
        this.idCounter = new Counter();
    }

    public static FuzzerClient getInstance() {
        if (fuzzerClient == null)
            return new FuzzerClient();
        else return fuzzerClient;
    }

    @Override
    public void run() {

    }

    public long generateId() {
        this.idCounter.incr();
        return this.idCounter.getValue();
    }

    public void interceptMessage(Message m) {
        this.messageMap.put(m.getId(), m);
        m.send();
    }

    public boolean isControlledExecution() {
        return false;
    }

    public ReentrantLock getLock() {
        return new ReentrantLock();
    }


}
