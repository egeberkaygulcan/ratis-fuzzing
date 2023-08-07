package org.apache.ratis.server.fuzzer.events;

import org.apache.ratis.server.fuzzer.FuzzerClient;

public abstract class Event {

    protected String type;
    protected String serverId;

    public abstract String toJsonString();

    public void setType(String type) {
        this.type = type;
    }

    public String getType() {
        return this.type;
    }

    public String toString() {
        return this.type;
    }
    
}
