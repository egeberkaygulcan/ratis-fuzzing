package org.apache.ratis.server.fuzzer.comm;

import com.google.gson.Gson;

public class JsonMessage {
    public String from;
    public String to;
    public byte[] data;
    public String type;

    public String id;

    public Boolean intercept = true;

    public JsonMessage(String to, String type, byte[] data) {
        this.to = to;
        this.data = data;
        this.type = type;
    }

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }

    public String getFrom() {
        return from;
    }

    public void setFrom(String from) {
        this.from = from;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getTo() {
        return to;
    }

    public void setTo(String to) {
        this.to = to;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String toJsonString() {
        Gson gson = GsonHelper.gson;
        return gson.toJson(this);
    }

    public static JsonMessage fromJsonString(String jsonString) {
        Gson gson = GsonHelper.gson;
        return gson.fromJson(jsonString, JsonMessage.class);
    }
}
