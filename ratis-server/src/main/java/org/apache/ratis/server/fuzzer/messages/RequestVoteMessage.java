package org.apache.ratis.server.fuzzer.messages;

import org.apache.ratis.proto.RaftProtos.RequestVoteRequestProto;
import org.apache.ratis.server.fuzzer.comm.GsonHelper;
import org.apache.ratis.server.fuzzer.comm.JsonMessage;
import org.apache.ratis.server.impl.LeaderElection;
import org.apache.ratis.server.impl.RaftServerImpl;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import org.apache.ratis.server.impl.LeaderElection.Executor;

public class RequestVoteMessage extends Message {

    private RequestVoteRequestProto request;
    private Executor executor;
    private RaftServerImpl server;

    public RequestVoteMessage(RequestVoteRequestProto r, Executor ex, RaftServerImpl s) {
        this.request = r;
        this.executor = ex;
        this.server = s;
        this.setType("request_vote_request");
        this.setId(this.client.generateId());

        isControlledExecution();
    }

    @Override
    public void invoke() {
        // election.notifyElection();
        this.executor.submit(() -> this.server.getServerRpc().requestVote(this.request));
    }

    @Override
    public String toJsonString() {
        String to = request.getServerRequest().getRequestorId().toStringUtf8();
        String from = request.getServerRequest().getReplyId().toStringUtf8();

        JsonObject json = new JsonObject();
        json.addProperty("type", type);
        json.addProperty("prevote", request.getPreVote());
        json.addProperty("term", (double) request.getCandidateTerm());
        json.addProperty("candidate_id", (double) client.getServerId(request.getServerRequest().getRequestorId().toStringUtf8()));
        json.addProperty("last_log_idx", (double) request.getCandidateLastEntry().getIndex());
        json.addProperty("last_log_term", (double) request.getCandidateLastEntry().getTerm());

        Gson gson = GsonHelper.gson;

        JsonMessage msg = new JsonMessage(Integer.toString(client.getServerId(to)), type, gson.toJson(json).getBytes());
        msg.setFrom(Integer.toString(client.getServerId(from)));
        msg.setId(Integer.toString(this.getId()));

        return gson.toJson(msg);
    }

    @Override
    public String getReceiver() {
        return this.request.getServerRequest().getReplyId().toString();
    }

    
}
