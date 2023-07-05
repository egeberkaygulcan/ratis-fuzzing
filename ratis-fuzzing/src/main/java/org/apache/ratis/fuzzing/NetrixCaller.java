package org.apache.ratis.server.impl.comm;

import okhttp3.*;
// import io.github.netrixframework.Event;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.gson.JsonObject;
import com.google.gson.Gson;

public class NetrixCaller {
    private OkHttpClient client = new OkHttpClient();
    private static MediaType JSON = MediaType.get("application/json; charset=utf-8");
    private NetrixClientConfig netrixClientConfig;
    private JsonObject replicaJson;

    public NetrixCaller(NetrixClientConfig netrixClientConfig) {
        this.netrixClientConfig = netrixClientConfig;
        JsonObject infoJson = new JsonObject();
        for(Entry<String, String> entry: netrixClientConfig.info.entrySet()) {
            infoJson.addProperty(entry.getKey(), entry.getValue());
        }
        JsonObject replicaJson = new JsonObject();
        replicaJson.addProperty("id", netrixClientConfig.replicaID);
        replicaJson.addProperty("ready", false);
        replicaJson.add("info", infoJson);
        replicaJson.addProperty("addr", netrixClientConfig.clientAdvAddr);
        this.replicaJson = replicaJson;
    }

    public void sendMessage(Message message) throws IOException {
        sendRequest("http://"+netrixClientConfig.netrixAddr+"/message", message.toJsonString());
    }

    // public void sendEvent(Event event) throws IOException {
    //     sendRequest("http://"+netrixClientConfig.netrixAddr+"/event", event.toJsonString());
    // }

    public void register() throws IOException {
        replicaJson.addProperty("ready", false);
        Gson gson = GsonHelper.gson;
        String replicaJsonString = gson.toJson(replicaJson);

        sendRequest("http://"+netrixClientConfig.netrixAddr+"/replica", replicaJsonString);
    }

    public void setReady() throws IOException {
        replicaJson.addProperty("ready", true);
        Gson gson = GsonHelper.gson;
        String replicaJsonString = gson.toJson(replicaJson);

        sendRequest("http://"+netrixClientConfig.netrixAddr+"/replica", replicaJsonString);
    }

    public void unsetReady() throws IOException {
        replicaJson.addProperty("ready", false);
        Gson gson = GsonHelper.gson;
        String replicaJsonString = gson.toJson(replicaJson);

        sendRequest("http://"+netrixClientConfig.netrixAddr+"/replica", replicaJsonString);
    }

    public void sendRequest(String url, String body) throws IOException {
        Request request = new Request.Builder()
                .url(url)
                .post(RequestBody.create(body, JSON))
                .build();
        Response response = null;
        try {
            response = client.newCall(request).execute();
        } finally {
            if (response != null) {
                response.close();
            }
        }
    }

}
