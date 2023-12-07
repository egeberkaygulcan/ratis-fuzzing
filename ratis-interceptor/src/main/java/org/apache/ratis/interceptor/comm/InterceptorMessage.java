package org.apache.ratis.interceptor.comm;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import org.apache.ratis.interceptor.InterceptorRpcService;
import org.apache.ratis.proto.RaftProtos.*;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftClientRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

public class InterceptorMessage {
    public static final Logger LOG = LoggerFactory.getLogger(InterceptorMessage.class);
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

    public String getType() {
        return this.type;
    }

    // Used to check if the message contains a request that the RPC service needs to poll for a reply
    public boolean needToWaitForReply() {
        return this.requestId != "" && (this.type == "request_vote_request" || this.type == "append_entries_request");
    }

    public String getRequestId() {
        return this.requestId;
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

    public RequestVoteRequestProto toRequestVoteRequest() throws IOException{
        return InterceptorMessageUtils.toRequestVoteRequest(this.data);
    }

    public RequestVoteReplyProto toRequestVoteReply() throws IOException{
        return InterceptorMessageUtils.toRequestVoteReply(this.data);
    }

    public AppendEntriesRequestProto toAppendEntriesRequest() throws IOException{
        return InterceptorMessageUtils.toAppendEntriesRequest(this.data);
    }

    public AppendEntriesReplyProto toAppendEntriesReply() throws IOException{
        return InterceptorMessageUtils.toAppendEntriesReply(this.data);
    }

    public InstallSnapshotRequestProto toInstallSnapshotRequest() throws IOException{
        return InterceptorMessageUtils.toInstallSnapshotRequest(this.data);
    }

    public InstallSnapshotReplyProto toInstallSnapshotReply() throws IOException{
        return InterceptorMessageUtils.toInstallSnapshotReply(this.data);
    }

    public StartLeaderElectionRequestProto toStartLeaderElectionRequest() throws IOException{
        return InterceptorMessageUtils.toStartLeaderElectionRequest(this.data);
    }

    public RaftClientRequest toRaftClientRequest() throws IOException{
        return InterceptorMessageUtils.toRaftClientRequest(this.data);
    }

    public RaftClientReply toRaftClientReply() throws IOException{
        return InterceptorMessageUtils.toRaftClientReply(this.data);
    }

    public StartLeaderElectionReplyProto toStartLeaderElectionReply() throws IOException{
        return InterceptorMessageUtils.toStartLeaderElectionReply(this.data);
    }

    public static class Builder {
        public static final Logger LOG = LoggerFactory.getLogger(Builder.class);
        private String from;
        private String id;
        private String requestId;

        private RequestVoteRequestProto requestVoteRequest;
        private RequestVoteReplyProto requestVoteReply;
        private AppendEntriesRequestProto appendEntriesRequest;
        private AppendEntriesReplyProto appendEntriesReply;
        private InstallSnapshotRequestProto installSnapshotRequest;
        private InstallSnapshotReplyProto installSnapshotReply;
        private StartLeaderElectionRequestProto startLeaderElectionRequest;
        private StartLeaderElectionReplyProto startLeaderElectionReply;
        private RaftClientRequest raftClientRequest;
        private RaftClientReply raftClientReply;


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

        public Builder setRaftClientRequest(RaftClientRequest raftClientRequest) {
            this.raftClientRequest = raftClientRequest;
            return this;
        }

        public Builder setRaftClientReply(RaftClientReply raftClientReply) {
            this.raftClientReply = raftClientReply;
            return this;
        }

        public InterceptorMessage build() throws IOException {
            byte[] data;
            String to;
            String type = "";
            
            if(this.requestVoteRequest != null) {
                data = InterceptorMessageUtils.fromRequestVoteRequest(this.requestVoteRequest);
                to = this.requestVoteRequest.getServerRequest().getReplyId().toStringUtf8();
                type = InterceptorMessageUtils.MessageType.RequestVoteRequest.toString();
            } else if(this.requestVoteReply != null) {
                data = InterceptorMessageUtils.fromRequestVoteReply(this.requestVoteReply);
                to = this.requestVoteReply.getServerReply().getRequestorId().toStringUtf8();
                type = InterceptorMessageUtils.MessageType.RequestVoteReply.toString();
            } else if(this.appendEntriesRequest != null) {
                data = InterceptorMessageUtils.fromAppendEntriesRequest(this.appendEntriesRequest);
                to = this.appendEntriesRequest.getServerRequest().getReplyId().toStringUtf8();
                type = "append_entries_request";
            } else if(this.appendEntriesReply != null) {
                data = InterceptorMessageUtils.fromAppendEntriesReply(this.appendEntriesReply);
                to = this.appendEntriesReply.getServerReply().getRequestorId().toStringUtf8();
                type = "append_entries_reply";
            } else if (this.installSnapshotRequest != null) {
                data = InterceptorMessageUtils.fromInstallSnapshotRequest(this.installSnapshotRequest);
                to = this.installSnapshotRequest.getServerRequest().getReplyId().toStringUtf8();
                type = InterceptorMessageUtils.MessageType.InstallSnapshotRequest.toString();
            } else if (this.installSnapshotReply != null) {
                data = InterceptorMessageUtils.fromInstallSnapshotReply(this.installSnapshotReply);
                to = this.installSnapshotReply.getServerReply().getRequestorId().toStringUtf8();
                type = InterceptorMessageUtils.MessageType.InstallSnapshotReply.toString();
            } else if (this.startLeaderElectionRequest != null) {
                data = InterceptorMessageUtils.fromStartLeaderElectionRequest(this.startLeaderElectionRequest);
                to = this.startLeaderElectionRequest.getServerRequest().getReplyId().toStringUtf8();
                type = InterceptorMessageUtils.MessageType.StartLeaderElectionRequest.toString();
            } else if (this.startLeaderElectionReply != null) {
                data = InterceptorMessageUtils.fromStartLeaderElectionReply(this.startLeaderElectionReply);
                to = this.startLeaderElectionReply.getServerReply().getRequestorId().toStringUtf8();
                type = InterceptorMessageUtils.MessageType.StartLeaderElectionReply.toString();
            } else if (this.raftClientRequest != null) {
                data = InterceptorMessageUtils.fromRaftClientRequest(this.raftClientRequest);
                to = this.raftClientRequest.getReplierId();
                type = InterceptorMessageUtils.MessageType.RaftClientRequest.toString();
            } else if (this.raftClientReply != null) {
                data = InterceptorMessageUtils.fromRaftClientReply(this.raftClientReply);
                to = this.raftClientReply.getRequestorId();
                type = InterceptorMessageUtils.MessageType.RaftClientReply.toString();
            } else {
                throw new IOException("invalid message type");
            }
            
            return new InterceptorMessage(this.from, to, type, data, id, requestId);
        }

        public InterceptorMessage buildWithJsonString(String jsonString) {
            LOG.info("Building message with Json string.");
            LOG.info(jsonString);

            try {
                JsonObject ob = JsonParser.parseString(jsonString).getAsJsonObject();
                InterceptorMessage.Builder builder = new InterceptorMessage.Builder();
                        // .setFrom(ob.get("from").getAsString())
                        // .setID(ob.get("id").getAsString())
                        // .setRequestId(ob.get("request_id").getAsString());
                
                InterceptorMessageUtils.MessageType messageType = InterceptorMessageUtils.MessageType.fromString(ob.get("type").getAsString());
                switch (messageType) {
                    case RequestVoteRequest:
                        builder.setRequestVoteRequest(InterceptorMessageUtils.toRequestVoteRequest(ob.get("data").getAsString().getBytes()));
                        break;
                    case RequestVoteReply:
                        builder.setRequestVoteReply(InterceptorMessageUtils.toRequestVoteReply(ob.get("data").getAsString().getBytes()));
                        break;
                    case AppendEntriesRequest:
                        builder.setAppendEntriesRequest(InterceptorMessageUtils.toAppendEntriesRequest(ob.get("data").getAsString().getBytes()));
                        break;
                    case AppendEntriesReply:
                        builder.setAppendEntriesReply(InterceptorMessageUtils.toAppendEntriesReply(ob.get("data").getAsString().getBytes()));
                        break;
                    case InstallSnapshotRequest:
                        builder.setInstallSnapshotRequest(InterceptorMessageUtils.toInstallSnapshotRequest(ob.get("data").getAsString().getBytes()));
                        break;
                    case InstallSnapshotReply:
                        builder.setInstallSnapshotReply(InterceptorMessageUtils.toInstallSnapshotReply(ob.get("data").getAsString().getBytes()));
                        break;
                    case StartLeaderElectionRequest:
                        builder.setStartLeaderElectionRequest(InterceptorMessageUtils.toStartLeaderElectionRequest(ob.get("data").getAsString().getBytes()));
                        break;
                    case StartLeaderElectionReply:
                        builder.setStartLeaderElectionReply(InterceptorMessageUtils.toStartLeaderElectionReply(ob.get("data").getAsString().getBytes()));
                        break;
                    case RaftClientRequest:
                        builder.setRaftClientRequest(InterceptorMessageUtils.toRaftClientRequest(ob.get("data").getAsString().getBytes()));
                        break;
                    case RaftClientReply:
                        builder.setRaftClientReply(InterceptorMessageUtils.toRaftClientReply(ob.get("data").getAsString().getBytes()));
                        break;
                    default:
                        break;
                }

                InterceptorMessage message = builder.build();
                LOG.info("Message built.");
                return message;
            } catch (IOException e) {
                LOG.error("Error while building with Json string: ", e);
            }

            return null;
        }
    }
}
