package org.github.fuzzing.messages;

import org.apache.ratis.proto.RaftProtos.RequestVoteReplyProto;
import org.apache.ratis.proto.RaftProtos.RequestVoteRequestProto;

public class RequestVoteReplyMessage extends Message {

    private RequestVoteReplyProto request;

    public RequestVoteReplyMessage(RequestVoteReplyProto r) {
        this.request = r;
        this.setType("request_vote_response");
        this.setId(this.client.generateId());
    }

    @Override
    public void invoke() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'invoke'");
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
