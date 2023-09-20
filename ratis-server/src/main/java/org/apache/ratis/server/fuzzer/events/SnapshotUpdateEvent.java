package org.apache.ratis.server.fuzzer.events;

import org.apache.ratis.server.fuzzer.comm.GsonHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

public class SnapshotUpdateEvent extends Event {

    public static final Logger LOG = LoggerFactory.getLogger(SnapshotUpdateEvent.class);
    private int index;

    public SnapshotUpdateEvent(int index, String serverId) {
        this.type = "UpdateSnapshot";
        this.serverId = serverId;
        this.index = index;

        LOG.info("New SnapshotUpdate event on server " + this.serverId + " to " + this.index);
    }

    @Override
    public String toJsonString() {
        JsonObject json = new JsonObject();
        json.addProperty("type", type);
        json.addProperty("server_id", serverId);
        json.addProperty("node", serverId);
        json.addProperty("snapshot_index", index);

        Gson gson = GsonHelper.gson;
        return gson.toJson(json);
    }
    
}
