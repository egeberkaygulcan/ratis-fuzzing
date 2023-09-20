package org.apache.ratis.server.fuzzer.events;

import org.apache.ratis.server.fuzzer.comm.GsonHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

public class ShutdownReadyEvent extends Event {

    public static final Logger LOG = LoggerFactory.getLogger(ShutdownReadyEvent.class);

    public ShutdownReadyEvent() {
        this.type = "ShutdownReady";
        LOG.info("Shutdown ready.");
    }

    @Override
    public String toJsonString() {
        JsonObject json = new JsonObject();
        json.addProperty("type", type);

        Gson gson = GsonHelper.gson;
        return gson.toJson(json);
    }
    
}
