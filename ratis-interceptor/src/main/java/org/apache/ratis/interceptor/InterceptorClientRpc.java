package org.apache.ratis.interceptor;

import com.squareup.okhttp.*;

import java.io.IOException;

import org.apache.ratis.client.RaftClientRpc;
import org.apache.ratis.client.impl.RaftClientRpcWithProxy;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.interceptor.comm.InterceptorMessage;
import org.apache.ratis.netty.NettyRpcProxy;
import org.apache.ratis.netty.client.NettyClientRpc;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftPeerId;

public class InterceptorClientRpc extends RaftClientRpcWithProxy<InterceptorRpcProxy> {
    public  InterceptorClientRpc(ClientId clientId, RaftProperties raftProperties) {
        super(new InterceptorRpcProxy.PeerMap(clientId.toString(), raftProperties));
    }

    @Override
    public RaftClientReply sendRequest(RaftClientRequest request) throws IOException {
        final RaftPeerId serverId = request.getServerId();
        final InterceptorRpcProxy proxy = getProxies().getProxy(serverId);

        String address = proxy.getPeerAddress();

        InterceptorMessage.Builder builder = new InterceptorMessage.Builder()
                .setRaftClientRequest(request)
                .setRequestId(request.getClientId().toString());
        InterceptorMessage msg = builder.build();

        OkHttpClient client = new OkHttpClient();

        MediaType JSON = MediaType.parse("application/json; charset=utf-8");
        Request httpRequest = new Request.Builder()
                .url(address)
                .post(RequestBody.create(JSON, msg.toJsonString()))
                .build();
        Response response = null;
        InterceptorMessage replyMsg = null;
        try {
            response = client.newCall(httpRequest).execute();
            if (response != null) {
                replyMsg = new InterceptorMessage.Builder().buildWithJsonString(response.body().string());
                if (replyMsg != null) {
                    return replyMsg.toRaftClientReply();
                }
            }
        } catch (IOException e) {
            // TODO: handle exception
        }

        return null;
    }
}
