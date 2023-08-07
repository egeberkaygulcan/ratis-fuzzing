package org.apache.ratis.server.fuzzer.messages;

import org.apache.ratis.server.fuzzer.FuzzerClient;

public abstract class Message {
    protected String messageId;
    protected String type;
    protected FuzzerClient fuzzerClient = FuzzerClient.getInstance();
    protected String invokeServerId;

    public abstract void invoke();

    public abstract String toJsonString();

    protected void isControlledExecution() {
        if (!this.fuzzerClient.isControlledExecution())
            invoke();
    }

    public void setId(String id) {
        this.messageId = id;
    }

    public String getId() {
        return this.messageId;
    }

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