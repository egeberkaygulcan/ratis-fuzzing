package org.apache.ratis.fuzzing.messages;

import org.apache.ratis.proto.RaftProtos.RequestVoteRequestProto;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.impl.LeaderElection;
import org.apache.ratis.server.impl.RaftServerImpl;
import org.apache.ratis.server.impl.LeaderElection.Executor;

public class RequestVoteMessage extends Message {

    private RequestVoteRequestProto request;
    private LeaderElection election;
    private Executor executor;
    private RaftServerImpl server;

    public RequestVoteMessage(RequestVoteRequestProto r, LeaderElection e, Executor ex, RaftServerImpl s) {
        this.request = r;
        this.election = e;
        this.executor = ex;
        this.server = s;
        this.setType("request_vote_request");
        this.setId(this.client.generateId());
    }

    @Override
    public void invoke() {
        election.unlock();
        this.executor.submit(() -> this.server.getServerRpc().requestVote(this.request));
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
        return this.request.getServerRequest().getReplyId().toString();
    }

    
}
