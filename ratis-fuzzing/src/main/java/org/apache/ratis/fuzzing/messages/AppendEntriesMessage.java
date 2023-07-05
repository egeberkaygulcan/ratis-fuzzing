package org.github.fuzzing.messages;

import org.apache.ratis.grpc.server.GrpcLogAppender.AppendEntriesRequest;
import org.apache.ratis.proto.RaftProtos.AppendEntriesRequestProto;

public class AppendEntriesMessage extends Message {

    private AppendEntriesRequestProto proto;
    private AppendEntriesRequest request;

    public AppendEntriesMessage(AppendEntriesRequestProto p, AppendEntriesRequest r) {
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
    
}
