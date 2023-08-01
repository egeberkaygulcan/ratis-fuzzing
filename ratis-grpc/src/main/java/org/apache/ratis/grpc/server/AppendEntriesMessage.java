package org.apache.ratis.grpc.server;

import org.apache.ratis.grpc.server.GrpcLogAppender.AppendEntriesRequest;
import org.apache.ratis.proto.RaftProtos.AppendEntriesRequestProto;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.server.fuzzer.comm.GsonHelper;
import org.apache.ratis.server.fuzzer.comm.JsonMessage;
import org.apache.ratis.server.fuzzer.messages.Message;
import org.apache.ratis.server.protocol.TermIndex;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

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
    public String toJsonString() {
        String to = proto.getServerRequest().getReplyId().toStringUtf8();
        String from = proto.getServerRequest().getRequestorId().toStringUtf8();

        JsonObject json = new JsonObject();
        TermIndex prevLog = request.getPreviousLog();
        long prev_log_idx = prevLog == null ? -1 : prevLog.getIndex();
        long prev_log_term = prevLog == null ? -1 : prevLog.getTerm();

        json.addProperty("type", type);
        json.addProperty("leader_id", client.getServerId(proto.getServerRequest().getRequestorId().toStringUtf8()));
        json.addProperty("term", (double) proto.getLeaderTerm());
        json.addProperty("prev_log_idx", (double) prev_log_idx);
        json.addProperty("prev_log_term", (double) prev_log_term);
        json.addProperty("leader_commit", (double) proto.getLeaderCommit());
        json.addProperty("msg_id", (double) messageId.intValue());

        JsonObject entries = new JsonObject();
        int i = 0;
        for(LogEntryProto entry : proto.getEntriesList()) {
            JsonObject entryJson = new JsonObject();
            entryJson.addProperty("term", (double) entry.getTerm());
            entryJson.addProperty("id", (int) entry.getIndex());
            entryJson.addProperty("session", ""); // Not applicable for Ratis
            entryJson.addProperty("type", -1); // Not applicable for Ratis
            entryJson.addProperty("data", entry.getLogEntryBodyCase().toString());
            entries.add(Integer.toString(i), entryJson);
            i++;
        }
        json.add("entries", entries);

        Gson gson = GsonHelper.gson;

        JsonMessage msg = new JsonMessage(Integer.toString(client.getServerId(to)), type, gson.toJson(json).getBytes());
        msg.setFrom(Integer.toString(client.getServerId(from)));
        msg.setId(Integer.toString(this.getId()));

        return gson.toJson(msg);
    }

    @Override
    public String getReceiver() {
        return this.proto.getServerRequest().getReplyId().toString();
    }
    
}
