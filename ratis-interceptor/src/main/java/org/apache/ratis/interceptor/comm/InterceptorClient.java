package org.apache.ratis.interceptor.comm;

import org.apache.ratis.server.RaftServer;

import java.io.IOException;
import java.net.InetSocketAddress;

public class InterceptorClient {
    // TODO
    //  [ ] Need to initiate a connection pool to the interceptor server
    //  [x] Need to define an message interface to communicate with the interceptor server
    //  [ ] Need to figure out how to start the listener server (own the thread? extend the thread interface?)
    //  [ ] Need to tag and keep track of requests and implement a future interface

    private RaftServer raftServer;

    private InetSocketAddress interceptorAddress;

    private InetSocketAddress listenAddress;

    private InterceptorServer listenServer;

    public InterceptorClient(RaftServer raftServer, InetSocketAddress interceptorAddress, InetSocketAddress listenAddress) {
        this.raftServer = raftServer;
        this.interceptorAddress = interceptorAddress;
        this.listenAddress = listenAddress;

        this.listenServer = new InterceptorServer(listenAddress);
    }

    public void start() throws IOException {}

    public void stop() throws IOException {}

    public void register() throws IOException {
    }

    public String getNewRequestId() {
        // TODO: need to use a concurrent counter here
        return "";
    }

    public String getNewMessageId() {
        // TODO: need to use a random number generator here to generate new IDs
        return "";
    }

    public void sendMessage(InterceptorMessage.Builder messageBuilder) {
        // TODO:
        //  [ ] need to return a future to wait for a message on
        //  [x] use the message builder to construct a message after assigning message id, from address
        InterceptorMessage message = messageBuilder.setID(getNewMessageId()).build();

    }

}
