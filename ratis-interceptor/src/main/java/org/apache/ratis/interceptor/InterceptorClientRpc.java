package org.apache.ratis.interceptor;

import org.apache.ratis.client.RaftClientRpc;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.netty.client.NettyClientRpc;
import org.apache.ratis.protocol.ClientId;

// TODO: need to reimplement to send http requests to the peer address
public class InterceptorClientRpc extends NettyClientRpc {
    public  InterceptorClientRpc(ClientId clientId, RaftProperties raftProperties) {
        super(clientId, raftProperties);
    }
}
