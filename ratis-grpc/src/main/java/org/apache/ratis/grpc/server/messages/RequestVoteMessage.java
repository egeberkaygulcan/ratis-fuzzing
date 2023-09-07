package org.apache.ratis.grpc.server.messages;

import org.apache.ratis.grpc.GrpcUtil;
import org.apache.ratis.proto.RaftProtos.RequestVoteReplyProto;
import org.apache.ratis.proto.RaftProtos.RequestVoteRequestProto;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.fuzzer.comm.GsonHelper;
import org.apache.ratis.server.fuzzer.comm.JsonMessage;
import org.apache.ratis.server.fuzzer.messages.Message;
import org.apache.ratis.thirdparty.io.grpc.stub.StreamObserver;
import org.apache.ratis.util.ProtoUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

public class RequestVoteMessage extends Message{

    public static final Logger LOG = LoggerFactory.getLogger(RequestVoteMessage.class);
    private RaftServer server;
    private RequestVoteRequestProto proto;
    private StreamObserver<RequestVoteReplyProto> responseObserver;

    public RequestVoteMessage(RaftServer server, RequestVoteRequestProto proto, StreamObserver<RequestVoteReplyProto> responseObserver, String invokeServerId) {
        this.type = "request_vote_request";
        this.messageId = fuzzerClient.generateId();
        this.server = server;
        this.proto = proto;
        this.responseObserver = responseObserver;
        this.invokeServerId = invokeServerId;

        LOG.info("Intercepted request vote from server " + this.proto.getServerRequest().getRequestorId().toStringUtf8() + " to " + this.proto.getServerRequest().getReplyId().toStringUtf8());

        if (!fuzzerClient.isControlledExecution())
            invoke();
    }

    @Override
    public void invoke() {
        if (fuzzerClient.shouldShutdown())
            return;
        // TODO - Test
        // if (!fuzzerClient.getElection())
        //     return;
        LOG.info("Invoking request vote on server: " + invokeServerId);
        try {
            final RequestVoteReplyProto reply = server.requestVote(proto);
            fuzzerClient.interceptMessage(new RequestVoteReplyMessage(reply, responseObserver, proto.getPreVote(), invokeServerId));
        } catch (Exception e) {
            GrpcUtil.warn(LOG, () -> getId() + ": Failed requestVote " + ProtoUtils.toString(proto.getServerRequest()), e);
            responseObserver.onError(GrpcUtil.wrapException(e));
        }
    }

    @Override
    public String toJsonString() {
        String to = proto.getServerRequest().getReplyId().toStringUtf8();
        String from = proto.getServerRequest().getRequestorId().toStringUtf8();

        JsonObject json = new JsonObject();
        json.addProperty("type", type);
        json.addProperty("prevote", proto.getPreVote());
        json.addProperty("term", (double) proto.getCandidateTerm());
        json.addProperty("candidate_id", (double) Integer.parseInt(proto.getServerRequest().getRequestorId().toStringUtf8()));
        json.addProperty("last_log_idx", (double) proto.getCandidateLastEntry().getIndex());
        json.addProperty("last_log_term", (double) proto.getCandidateLastEntry().getTerm());

        Gson gson = GsonHelper.gson;

        JsonMessage msg = new JsonMessage(to, type, gson.toJson(json).getBytes());
        msg.setFrom(from);
        msg.setId(this.getId());

        return gson.toJson(msg);
    }
    
}
