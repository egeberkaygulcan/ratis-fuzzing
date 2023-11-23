package org.apache.ratis.interceptor;

import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.interceptor.comm.InterceptorMessage;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.util.PeerProxyMap;

import java.io.Closeable;
import java.io.IOException;

// The proxy server can use the listen address to communicate the message to the 
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

    public InterceptorRpcProxy(RaftPeer peer, RaftProperties raftProperties) {
        this.peer = peer;
    }

    @Override
    public void close() {}

    public InterceptorMessage send() {
        return null;
    }
}
