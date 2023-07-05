package org.apache.ratis.grpc.server;

import org.apache.ratis.grpc.server.GrpcLogAppender.AppendEntriesRequest;
import org.apache.ratis.proto.RaftProtos.AppendEntriesRequestProto;
import org.apache.ratis.server.fuzzer.messages.Message;

public class AppendEntriesMessage extends Message {

    private AppendEntriesRequestProto proto;
    private AppendEntriesRequest request;
    private boolean heartbeat;
    private GrpcLogAppender logAppender;

    public AppendEntriesMessage(AppendEntriesRequestProto p, AppendEntriesRequest r, boolean h, GrpcLogAppender g) {
        this.proto = p;
        this.request = r;
        this.heartbeat = h;
        this.logAppender = g;
        this.setType("append_entries_request");
        this.setId(this.client.generateId());

        isControlledExecution();
    }

    @Override
    public void invoke() {
        try {
            this.logAppender.appendControlledLog(this.request, this.proto, this.heartbeat);
        } catch (Exception e) {
            e.printStackTrace();
        }
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
