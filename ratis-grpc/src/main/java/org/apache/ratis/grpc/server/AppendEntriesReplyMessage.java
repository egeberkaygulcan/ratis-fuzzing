package org.apache.ratis.grpc.server;

import org.apache.ratis.grpc.server.GrpcLogAppender.AppendLogResponseHandler;
import org.apache.ratis.proto.RaftProtos.AppendEntriesReplyProto;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.server.fuzzer.comm.GsonHelper;
import org.apache.ratis.server.fuzzer.comm.JsonMessage;
import org.apache.ratis.server.fuzzer.messages.Message;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

public class AppendEntriesReplyMessage extends Message {

    private AppendEntriesReplyProto request;
    private AppendLogResponseHandler responseHandler;

    public AppendEntriesReplyMessage(AppendEntriesReplyProto r, AppendLogResponseHandler a) {
        this.request = r;
        this.responseHandler = a;
        this.setType("append_entries_response");
        this.setId(this.client.generateId());

        isControlledExecution();
    }

    @Override
    public void invoke() {
        this.responseHandler.onNextInvoke(this.request);
    }

    @Override
    public String toJsonString() {
        String to = request.getServerReply().getRequestorId().toStringUtf8();
        String from = request.getServerReply().getReplyId().toStringUtf8();

        JsonObject json = new JsonObject();
        json.addProperty("type", type);
        json.addProperty("success", request.getServerReply().getSuccess());
        json.addProperty("term", (double) request.getTerm());
        json.addProperty("current_idx", (double) request.getMatchIndex());
        json.addProperty("msg_id", (double) messageId.intValue());

        Gson gson = GsonHelper.gson;

        JsonMessage msg = new JsonMessage(to, type, gson.toJson(json).getBytes());
        msg.setFrom(from);
        msg.setId(Integer.toString(this.getId()));

        return gson.toJson(msg);
    }

    @Override
    public String getReceiver() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getReceiver'");
    }

    public boolean getHearbeat() {
        return request.getIsHearbeat();
    }
    
}
