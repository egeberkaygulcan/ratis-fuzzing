package org.apache.ratis.interceptor.comm;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.apache.ratis.proto.RaftProtos.*;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

public class InterceptorMessage {
    private final String from;
    private final String to;
    private final byte[] data;
    private final String type;
    private final String id;
    private final String requestId;

    private final Map<String, Object> params;

    private InterceptorMessage(String from, String to, String type, byte[] data, String id) {
        this.from = from;
        this.to = to;
        this.data = data;
        this.type = type;
        this.id = id;
        this.requestId = "";
        this.params = new HashMap<>();
    }

    private InterceptorMessage(String from, String to, String type, byte[] data, String id, String requestId) {
        this.from = from;
        this.to = to;
        this.data = data;
        this.type = type;
        this.id = id;
        this.requestId = requestId;
        this.params = new HashMap<>();
    }

    public void setParam(String key, Object value) {
        this.params.put(key, value);
    }

    public String getFrom() {
        return this.from;
    }

    public String getTo() {
        return this.to;
    }

    public String toJsonString() {
        JsonObject json = new JsonObject();
        json.addProperty("from", this.from);
        json.addProperty("to", this.to);
        json.addProperty("type", this.type);
        json.addProperty("id", this.id);

        JsonObject dataJson = new JsonObject();
        dataJson.addProperty("data", Base64.getEncoder().encodeToString(this.data));
        dataJson.addProperty("request_id", this.requestId);

        Gson gson = new GsonBuilder().create();
        JsonElement paramsJson = gson.toJsonTree(this.params);

        byte[] dataBytes = gson.toJson(dataJson).getBytes();

        json.addProperty("data", Base64.getEncoder().encodeToString(dataBytes));
        json.add("params", paramsJson);

        return gson.toJson(json);
    }

    public static class Builder {
        private String from;
        private String id;
        private String requestId;
        private String type;

        private RequestVoteRequestProto requestVoteRequest;
        private RequestVoteReplyProto requestVoteReply;
        private AppendEntriesRequestProto appendEntriesRequest;
        private AppendEntriesReplyProto appendEntriesReply;
        private InstallSnapshotRequestProto installSnapshotRequest;
        private InstallSnapshotReplyProto installSnapshotReply;
        private StartLeaderElectionRequestProto startLeaderElectionRequest;
        private StartLeaderElectionReplyProto startLeaderElectionReply;

        public Builder() {}

        public Builder setFrom(String from) {
            this.from = from;
            return this;
        }

        public Builder setID(String id) {
            this.id = id;
            return this;
        }

        public Builder setRequestId(String requestId) {
            this.requestId = requestId;
            return this;
        }

        public Builder setAppendEntriesRequest(AppendEntriesRequestProto appendEntriesRequest) {
            this.appendEntriesRequest = appendEntriesRequest;
            return this;
        }

        public Builder setAppendEntriesReply(AppendEntriesReplyProto appendEntriesReply) {
            this.appendEntriesReply = appendEntriesReply;
            return this;
        }

        public Builder setRequestVoteRequest(RequestVoteRequestProto requestVoteRequest) {
            this.requestVoteRequest = requestVoteRequest;
            return this;
        }

        public Builder setRequestVoteReply(RequestVoteReplyProto requestVoteReply) {
            this.requestVoteReply = requestVoteReply;
            return this;
        }

        public Builder setInstallSnapshotRequest(InstallSnapshotRequestProto installSnapshotRequest) {
            this.installSnapshotRequest = installSnapshotRequest;
            return this;
        }

        public Builder setInstallSnapshotReply(InstallSnapshotReplyProto installSnapshotReply) {
            this.installSnapshotReply = installSnapshotReply;
            return this;
        }
        public Builder setStartLeaderElectionRequest(StartLeaderElectionRequestProto startLeaderElectionRequest) {
            this.startLeaderElectionRequest = startLeaderElectionRequest;
            return this;
        }
        public Builder setStartLeaderElectionReply(StartLeaderElectionReplyProto startLeaderElectionReply) {
            this.startLeaderElectionReply = startLeaderElectionReply;
            return this;
        }

        public InterceptorMessage build() {
            // TODO: construct the data encoding based on each message type
            return null;
        }

        public InterceptorMessage buildWithJsonString(String jsonString) {
            // TODO: deserializing the json to InterceptorMessage
            return null;
        }
    }
}
