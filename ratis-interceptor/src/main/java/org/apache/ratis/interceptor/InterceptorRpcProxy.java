package org.apache.ratis.interceptor;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.squareup.okhttp.*;

import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.interceptor.comm.InterceptorMessage;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.util.PeerProxyMap;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

// The proxy initiates a communication to each peer and is used to communicate to that peer
// We need the proxy to send messages that are not intercepted
public class InterceptorRpcProxy implements Closeable {
    public static class PeerMap extends PeerProxyMap<InterceptorRpcProxy> {
        private final RaftProperties properties;

        public PeerMap(String name, RaftProperties properties) {
            super(name);
            this.properties = properties;
        }

        @Override
        public InterceptorRpcProxy createProxyImpl(RaftPeer peer)
                throws IOException {
            return new InterceptorRpcProxy(peer, properties);
        }

        @Override
        public void close() {
            super.close();
        }
    }

    private final RaftPeer peer;
    private OkHttpClient client = new OkHttpClient();

    public InterceptorRpcProxy(RaftPeer peer, RaftProperties raftProperties) {
        this.peer = peer;
    }

    @Override
    public void close() {}

    public InterceptorMessage send(InterceptorMessage request) {
        // TODO:
        //  [X] need to figure out the right address to the peer and send a http request to that peer
        String address = this.peer.getAddress();
        String json = request.toJsonString();

        MediaType JSON = MediaType.parse("application/json; charset=utf-8");
        Request httpRequest = new Request.Builder()
                .url(address)
                .post(RequestBody.create(JSON, json))
                .build();
        Response response = null;
        InterceptorMessage msg = null;
        try {
            response = client.newCall(httpRequest).execute();
            if (response != null) {
                msg = new InterceptorMessage.Builder().buildWithJsonString(response.body().string());
            }
        } catch (IOException e) {
            // TODO: handle exception
        }
        if (msg != null) {
            return msg;
        }

        return null;
    }
}
