package org.apache.ratis.server.fuzzer.events;

import javax.swing.plaf.nimbus.State;

import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.fuzzer.FuzzerClient;
import org.apache.ratis.server.fuzzer.comm.GsonHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

public class StateChangeEvent extends Event {

    public static final Logger LOG = LoggerFactory.getLogger(StateChangeEvent.class);
    private String newState;
    private FuzzerClient fuzzerClient = FuzzerClient.getInstance();

    public StateChangeEvent(String newState, String serverId) {
        this.type = "state_change";
        this.serverId = serverId;
        this.newState = newState;

        LOG.info("New state change event on server " + this.serverId + " to " + this.newState);
    }

    @Override
    public String toJsonString() {
        JsonObject json = new JsonObject();
        json.addProperty("type", type);
        json.addProperty("server_id", serverId);
        json.addProperty("new_state", newState);

        Gson gson = GsonHelper.gson;
        return gson.toJson(json);
    }
    
}
