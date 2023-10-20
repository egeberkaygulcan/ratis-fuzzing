package org.apache.ratis.server.fuzzer.events;

import java.util.Base64;
import java.util.List;

import javax.swing.plaf.nimbus.State;

import org.apache.ratis.proto.RaftProtos.AppendEntriesRequestProto;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.fuzzer.FuzzerClient;
import org.apache.ratis.server.fuzzer.comm.GsonHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

public class LogUpdateEvent extends Event {

    public static final Logger LOG = LoggerFactory.getLogger(LogUpdateEvent.class);
    private long logIndex;

    public LogUpdateEvent(RaftPeerId serverId, long logIndex) {
        this.type = "LogUpdate";
        this.serverId = serverId.toString();
        this.logIndex = logIndex;

        LOG.info("New log update event on server " + this.serverId);
    }

    @Override
    public String toJsonString() {
        JsonObject json = new JsonObject();
        json.addProperty("type", type);
        json.addProperty("server_id", serverId);
        json.addProperty("log_index", logIndex);
        
        // JsonObject entries_ = new JsonObject();
        // int i = 0;
        // String entryStr;
        // for(LogEntryProto entry : entries) {
        //     if (entry.hasStateMachineLogEntry()) {
        //         JsonObject entryJson = new JsonObject();
        //         entryStr = Base64.getEncoder().encodeToString(entry.getStateMachineLogEntry().getLogData().toByteArray());
        //         entryJson.addProperty("data_len", entryStr.length());
        //         if (entryStr.length() > 0) {
        //             entryJson.addProperty("data", entryStr);
        //         } else {
        //             entryJson.addProperty("data", "");
        //         }
        //         entries_.add(Integer.toString(i), entryJson);
        //         i++;
        //     }
        // }

        Gson gson = GsonHelper.gson;
        return gson.toJson(json);
    }
    
}
