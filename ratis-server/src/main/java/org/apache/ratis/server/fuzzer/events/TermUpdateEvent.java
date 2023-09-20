package org.apache.ratis.server.fuzzer.events;

import javax.swing.plaf.nimbus.State;

import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.fuzzer.FuzzerClient;
import org.apache.ratis.server.fuzzer.comm.GsonHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

public class TermUpdateEvent extends Event {

    public static final Logger LOG = LoggerFactory.getLogger(TermUpdateEvent.class);
    private int term;

    public TermUpdateEvent(String serverId, int term) {
        this.type = "commit_update";
        this.serverId = serverId;
        this.term = term;

        LOG.info("New term update event on server " + this.serverId + " to " + this.term);
    }

    @Override
    public String toJsonString() {
        JsonObject json = new JsonObject();
        json.addProperty("type", type);
        json.addProperty("server_id", serverId);
        json.addProperty("term", term);

        Gson gson = GsonHelper.gson;
        return gson.toJson(json);
    }
    
}
