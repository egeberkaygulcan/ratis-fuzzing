package org.apache.ratis.server.fuzzer.events;

import org.apache.ratis.server.fuzzer.comm.GsonHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

public class ClientRequestEvent extends Event {

    public static final Logger LOG = LoggerFactory.getLogger(StateChangeEvent.class);

    public ClientRequestEvent(String serverId) {
        this.type = "ClientRequest";
        this.serverId = serverId;
        LOG.info("New ClientRequest event on server " + this.serverId);
    }

    @Override
    public String toJsonString() {
        JsonObject json = new JsonObject();
        json.addProperty("type", type);
        json.addProperty("server_id", serverId);
        json.addProperty("leader", serverId);

        Gson gson = GsonHelper.gson;
        return gson.toJson(json);
    }
    
}
