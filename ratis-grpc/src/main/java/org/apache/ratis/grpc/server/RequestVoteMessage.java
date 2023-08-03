package org.apache.ratis.grpc.server;

import org.apache.ratis.proto.RaftProtos.RequestVoteReplyProto;
import org.apache.ratis.proto.RaftProtos.RequestVoteRequestProto;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.fuzzer.comm.GsonHelper;
import org.apache.ratis.server.fuzzer.comm.JsonMessage;
import org.apache.ratis.server.fuzzer.messages.Message;
import org.apache.ratis.thirdparty.io.grpc.stub.StreamObserver;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

public class RequestVoteMessage extends Message {

    private RequestVoteRequestProto request;
    private GrpcServerProtocolService service;
    StreamObserver<RequestVoteReplyProto> responseObserver;

    public RequestVoteMessage(RequestVoteRequestProto r, GrpcServerProtocolService service, StreamObserver<RequestVoteReplyProto> responseObserver) {
        this.request = r;
        this.service = service;
        this.responseObserver = responseObserver;
        this.setType("request_vote_request");
        this.setId(this.client.generateId());

        isControlledExecution();
    }

    @Override
    public void invoke() {
        service.sendVoteRequest(request, responseObserver);
    }

    @Override
    public String toJsonString() {
        String to = request.getServerRequest().getRequestorId().toStringUtf8();
        String from = request.getServerRequest().getReplyId().toStringUtf8();

        JsonObject json = new JsonObject();
        json.addProperty("type", type);
        json.addProperty("prevote", request.getPreVote());
        json.addProperty("term", (double) request.getCandidateTerm());
        json.addProperty("candidate_id", (double) Integer.parseInt(request.getServerRequest().getRequestorId().toStringUtf8()));
        json.addProperty("last_log_idx", (double) request.getCandidateLastEntry().getIndex());
        json.addProperty("last_log_term", (double) request.getCandidateLastEntry().getTerm());

        Gson gson = GsonHelper.gson;

        JsonMessage msg = new JsonMessage(to, type, gson.toJson(json).getBytes());
        msg.setFrom(from);
        msg.setId(Integer.toString(this.getId()));

        return gson.toJson(msg);
    }

    @Override
    public String getReceiver() {
        return this.request.getServerRequest().getReplyId().toStringUtf8();
    }

    
}
