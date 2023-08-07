package org.apache.ratis.grpc.server.messages;

import org.apache.ratis.grpc.server.GrpcLogAppender.AppendLogResponseHandler;
import org.apache.ratis.proto.RaftProtos.AppendEntriesReplyProto;
import org.apache.ratis.server.fuzzer.comm.GsonHelper;
import org.apache.ratis.server.fuzzer.comm.JsonMessage;
import org.apache.ratis.server.fuzzer.messages.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

public class AppendEntriesReplyMessage extends Message {

    public static final Logger LOG = LoggerFactory.getLogger(AppendEntriesReplyMessage.class);
    private AppendEntriesReplyProto proto;
    private AppendLogResponseHandler responseHandler;

    public AppendEntriesReplyMessage(AppendEntriesReplyProto proto, AppendLogResponseHandler responseHandler, String invokeServerId) {
        this.type = "append_entries_response";
        this.messageId = fuzzerClient.generateId();
        this.proto = proto;
        this.responseHandler = responseHandler;
        this.invokeServerId = invokeServerId;

        LOG.info("Intercepted append entries reply from server " + this.proto.getServerReply().getReplyId().toStringUtf8() + " to " + this.proto.getServerReply().getRequestorId().toStringUtf8() + " with id: " + this.getId());
    
        if (!fuzzerClient.isControlledExecution())
            invoke();
    }

    @Override
    public void invoke() {
        if (fuzzerClient.shouldShutdown())
            return;
        LOG.info("Invoking append entries reply on server: " + invokeServerId);
        responseHandler.onNext_(proto);
    }

    @Override
    public String toJsonString() {
        String to = proto.getServerReply().getRequestorId().toStringUtf8();
        String from = proto.getServerReply().getReplyId().toStringUtf8();

        JsonObject json = new JsonObject();
        json.addProperty("type", type);
        json.addProperty("success", proto.getServerReply().getSuccess());
        json.addProperty("term", (double) proto.getTerm());
        json.addProperty("current_idx", (double) proto.getMatchIndex());
        json.addProperty("msg_id", (double) -1);

        Gson gson = GsonHelper.gson;

        JsonMessage msg = new JsonMessage(to, type, gson.toJson(json).getBytes());
        msg.setFrom(from);
        msg.setId(this.getId());

        return gson.toJson(msg);
    }
    
}
