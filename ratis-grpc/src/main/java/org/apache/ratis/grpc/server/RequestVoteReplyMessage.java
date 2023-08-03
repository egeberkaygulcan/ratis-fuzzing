package org.apache.ratis.grpc.server;

import org.apache.ratis.proto.RaftProtos.RequestVoteReplyProto;
import org.apache.ratis.server.fuzzer.comm.GsonHelper;
import org.apache.ratis.server.fuzzer.comm.JsonMessage;
import org.apache.ratis.server.fuzzer.messages.Message;
import org.apache.ratis.thirdparty.io.grpc.stub.StreamObserver;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

public class RequestVoteReplyMessage extends Message {

    private RequestVoteReplyProto request;
    StreamObserver<RequestVoteReplyProto> responseObserver;

    public RequestVoteReplyMessage(RequestVoteReplyProto r, StreamObserver<RequestVoteReplyProto> ro) {
        this.request = r;
        this.responseObserver = ro;
        this.setType("request_vote_response");
        this.setId(this.client.generateId());

        isControlledExecution();
    }

    @Override
    public void invoke() {
        try {
            this.responseObserver.onNext(this.request);
            this.responseObserver.onCompleted();
        } catch (Exception e) {
            System.out.println("RequestVoteReply failed");
            e.printStackTrace();

        }
    }

    @Override
    public String getReceiver() {
        return null;
    }

    @Override
    public String toJsonString() {
        String to = request.getServerReply().getRequestorId().toStringUtf8();
        String from = request.getServerReply().getReplyId().toStringUtf8();

        JsonObject json = new JsonObject();
        json.addProperty("type", type);
        json.addProperty("term", (double) request.getTerm());
        json.addProperty("prevote", false);
        json.addProperty("request_term", (double) request.getTerm());
        json.addProperty("vote_granted", request.getServerReply().getSuccess());

        Gson gson = GsonHelper.gson;

        JsonMessage msg = new JsonMessage(to, type, gson.toJson(json).getBytes());
        msg.setFrom(from);
        msg.setId(Integer.toString(this.getId()));

        return gson.toJson(msg);
    }
    
}
