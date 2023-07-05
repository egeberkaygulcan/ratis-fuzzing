package org.apache.ratis.fuzzing.messages;

import org.apache.ratis.proto.RaftProtos.RequestVoteRequestProto;

public class RequestVoteMessage extends Message {

    private RequestVoteRequestProto request;

    public RequestVoteMessage(RequestVoteRequestProto r) {
        this.request = r;
        this.setType("request_vote_request");
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
