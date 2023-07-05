package org.github.fuzzing.messages;

import org.apache.ratis.proto.RaftProtos.RequestVoteReplyProto;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.server.impl.RaftServerImpl;

public class TimeoutMessage extends Message {

    RaftServerImpl server;

    public TimeoutMessage(RaftServerImpl s) {
        this.server = s;
        this.setType("timeout");
        this.setId(this.client.generateId());
    
        isControlledExecution();
    }

    @Override
    public void invoke() {
        this.server.changeToCandidate(false);
    }

    @Override
    public void send() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'send'");
    }

    @Override
    protected String toJsonString() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'toJsonString'");
    }
    
}
