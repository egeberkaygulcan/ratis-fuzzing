package org.apache.ratis.server.fuzzer.events;

import org.apache.ratis.server.fuzzer.comm.GsonHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

public class AskLeaderEvent extends Event {

    public static final Logger LOG = LoggerFactory.getLogger(StateChangeEvent.class);

    public AskLeaderEvent() {
        this.type = "ask_leader_id";
        this.serverId = "client";
        LOG.info("New ask leader event on server ");
    }

    @Override
    public String toJsonString() {
        JsonObject json = new JsonObject();
        json.addProperty("type", type);
        json.addProperty("server_id", serverId);

        Gson gson = GsonHelper.gson;
        return gson.toJson(json);
    }
    
}
