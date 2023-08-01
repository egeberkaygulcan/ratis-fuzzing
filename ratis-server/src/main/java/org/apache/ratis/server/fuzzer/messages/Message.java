package org.apache.ratis.server.fuzzer.messages;

import org.apache.ratis.server.fuzzer.FuzzerClient;

public abstract class Message {
    protected Integer messageId;
    protected String type;
    protected FuzzerClient client = FuzzerClient.getInstance("Message.java");

    public abstract void invoke();

    public abstract String getReceiver();

    public abstract String toJsonString();

    protected void isControlledExecution() {
        if (!this.client.isControlledExecution())
            invoke();
    }

    public void setId(Integer id) {
        this.messageId = id;
    }

    public Integer getId() {
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
