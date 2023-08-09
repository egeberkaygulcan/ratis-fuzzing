package org.apache.ratis.server.fuzzer.events;

import javax.swing.plaf.nimbus.State;

import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.fuzzer.FuzzerClient;
import org.apache.ratis.server.fuzzer.comm.GsonHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

public class CommitUpdateEvent extends Event {

    public static final Logger LOG = LoggerFactory.getLogger(StateChangeEvent.class);
    private int commitIndex;

    public CommitUpdateEvent(String serverId, int commitIndex) {
        this.type = "commit_update";
        this.serverId = serverId;
        this.commitIndex = commitIndex;

        LOG.info("New commit update event on server " + this.serverId + " to " + this.commitIndex);
    }

    @Override
    public String toJsonString() {
        JsonObject json = new JsonObject();
        json.addProperty("type", type);
        json.addProperty("server_id", serverId);
        json.addProperty("commit_index", commitIndex);

        Gson gson = GsonHelper.gson;
        return gson.toJson(json);
    }
    
}
