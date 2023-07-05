package org.apache.ratis.server.fuzzer.messages;

import org.apache.ratis.proto.RaftProtos.AppendEntriesReplyProto;
import org.apache.ratis.proto.RaftProtos.RequestVoteReplyProto;

public class AppendEntriesReplyMessage extends Message {

    private AppendEntriesReplyProto request;

    public AppendEntriesReplyMessage(AppendEntriesReplyProto r) {
        this.request = r;
        this.setType("append_entries_response");
        this.setId(this.client.generateId());

        isControlledExecution();
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

    @Override
    public String getReceiver() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getReceiver'");
    }
    
}
