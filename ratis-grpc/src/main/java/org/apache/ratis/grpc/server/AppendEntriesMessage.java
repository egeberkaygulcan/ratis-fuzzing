package org.apache.ratis.fuzzing.messages;

import org.apache.ratis.proto.RaftProtos.AppendEntriesRequestProto;
import org.apache.ratis.server.fuzzer.messages.Message;

public class AppendEntriesMessage extends Message {

    private AppendEntriesRequestProto proto;
    private Object request;

    public AppendEntriesMessage(AppendEntriesRequestProto p, Object request) {
        this.proto = p;
        this.request = r;
        this.setType("append_entries_request");
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

    @Override
    public String getReceiver() {
        return this.proto.getServerRequest().getReplyId().toString();
    }
    
}
