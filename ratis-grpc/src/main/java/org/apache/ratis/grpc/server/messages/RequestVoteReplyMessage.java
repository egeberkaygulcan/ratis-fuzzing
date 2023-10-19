package org.apache.ratis.grpc.server.messages;

import org.apache.ratis.grpc.GrpcUtil;
import org.apache.ratis.proto.RaftProtos.RequestVoteReplyProto;
import org.apache.ratis.server.fuzzer.comm.GsonHelper;
import org.apache.ratis.server.fuzzer.comm.JsonMessage;
import org.apache.ratis.server.fuzzer.messages.Message;
import org.apache.ratis.thirdparty.io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

public class RequestVoteReplyMessage extends Message {

    public static final Logger LOG = LoggerFactory.getLogger(RequestVoteReplyMessage.class);
    private RequestVoteReplyProto proto;
    private StreamObserver<RequestVoteReplyProto> responseObserver;
    private boolean prevote;


    public RequestVoteReplyMessage(RequestVoteReplyProto proto, StreamObserver<RequestVoteReplyProto> responseObserver, boolean prevote, String invokeServerId) {
        this.type = "request_vote_response";
        this.messageId = fuzzerClient.generateId();
        this.proto = proto;
        this.responseObserver = responseObserver;
        this.prevote = prevote;
        this.invokeServerId = invokeServerId;

        LOG.info("Intercepted request vote reply from server " + this.proto.getServerReply().getReplyId().toStringUtf8() + " to " + this.proto.getServerReply().getRequestorId().toStringUtf8());
        LOG.info("responseObserver name: " + responseObserver.getClass().getName() + " , " + responseObserver.getClass().getSimpleName());

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
        LOG.info("Invoking request vote reply on server: " + invokeServerId);
        try {
            responseObserver.onNext(proto);
            responseObserver.onCompleted();
          } catch (Exception e) {
            responseObserver.onError(GrpcUtil.wrapException(e));
        }
    }

    @Override
    public String toJsonString() {
        String to = proto.getServerReply().getRequestorId().toStringUtf8();
        String from = proto.getServerReply().getReplyId().toStringUtf8();

        JsonObject json = new JsonObject();
        json.addProperty("type", type);
        json.addProperty("term", (double) proto.getTerm());
        json.addProperty("prevote", prevote);
        json.addProperty("request_term", (double) proto.getTerm());
        int vote_granted = proto.getServerReply().getSuccess() ? 1 : 0;
        json.addProperty("vote_granted", vote_granted);

        Gson gson = GsonHelper.gson;

        JsonMessage msg = new JsonMessage(to, type, gson.toJson(json).getBytes());
        msg.setFrom(from);
        msg.setId(this.getId());

        return gson.toJson(msg);
    }
    
}
