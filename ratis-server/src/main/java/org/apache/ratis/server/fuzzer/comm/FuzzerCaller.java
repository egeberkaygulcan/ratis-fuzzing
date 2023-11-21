package org.apache.ratis.server.fuzzer.comm;

import okhttp3.*;

import java.io.IOException;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonObject;
import com.google.gson.Gson;

public class FuzzerCaller {

    Logger LOG = LoggerFactory.getLogger(FuzzerCaller.class);

    private OkHttpClient client = new OkHttpClient();
    private static MediaType JSON = MediaType.get("application/json; charset=utf-8");
    private String fuzzerAddress;

    public FuzzerCaller(String fuzzerAddress) {
        this.fuzzerAddress = fuzzerAddress;
    }

    public void updateFuzzerAddress(String address) {
        this.fuzzerAddress = address;
    }


    public void sendMessage(String message) throws IOException {
        sendRequest("http://"+fuzzerAddress+"/message", message);
    }

    public void sendEvent(String event) throws IOException {
        sendRequest("http://"+fuzzerAddress+"/event", event);
    }

    public void sendReplica(String replica) throws IOException {
        sendRequest("http://"+fuzzerAddress+"/replica", replica);
    }

    public void unsetReady() throws IOException {
        // replicaJson.addProperty("ready", false);
        // Gson gson = GsonHelper.gson;
        // String replicaJsonString = gson.toJson(replicaJson);

        // sendRequest("http://"+fuzzerAddress+"/replica", readyJson);
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
