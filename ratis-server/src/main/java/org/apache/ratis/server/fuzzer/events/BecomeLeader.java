package org.apache.ratis.server.fuzzer.events;

import org.apache.ratis.server.fuzzer.comm.GsonHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

public class BecomeLeader extends Event {

    public static final Logger LOG = LoggerFactory.getLogger(StateChangeEvent.class);
    private int term;

    public BecomeLeader(int term, String serverId) {
        this.type = "BecomeLeader";
        this.serverId = serverId;
        this.term = term;
        LOG.info("New BecomeLeader event on server " + this.serverId);
    }

    @Override
    public String toJsonString() {
        JsonObject json = new JsonObject();
        json.addProperty("type", type);
        json.addProperty("server_id", serverId);
        json.addProperty("node", serverId);
        json.addProperty("term", term);

        Gson gson = GsonHelper.gson;
        return gson.toJson(json);
    }
    
}
