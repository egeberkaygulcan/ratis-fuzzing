package org.apache.ratis.server.fuzzer.messages;

import org.apache.ratis.proto.RaftProtos.RequestVoteReplyProto;
import org.apache.ratis.server.impl.RaftServerImpl;

public class TimeoutMessage extends Message {

    RaftServerImpl server;

    public TimeoutMessage(RaftServerImpl s) {
        this.server = s;
        this.setType("timeout");
        this.setId(this.client.generateId());
    
        isControlledExecution();
        invoke();
    }

    @Override
    public void invoke() {
        this.server.changeToCandidate(false);
    }

    @Override
    public String toJsonString() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'toJsonString'");
    }

    @Override
    public String getReceiver() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getReceiver'");
    }
    
}
