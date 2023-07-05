package org.github.fuzzing.messages;

import org.apache.ratis.server.impl.comm.FuzzerClient;

public abstract class Message {
    protected long messageId;
    protected String type;
    protected FuzzerClient client = FuzzerClient.getInstance();

    public abstract void invoke();

    public abstract void send();

    protected abstract String toJsonString();

    protected void isControlledExecution() {
        if (!this.client.isControlledExecution())
            invoke();
    }

    public void setId(long id) {
        this.messageId = id;
    }

    public long getId() {
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
