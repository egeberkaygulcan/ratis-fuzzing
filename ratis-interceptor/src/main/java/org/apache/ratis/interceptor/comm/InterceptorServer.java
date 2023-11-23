package org.apache.ratis.interceptor.comm;

import java.net.InetSocketAddress;
import java.util.List;

public class InterceptorServer {
    // TODO:
    //  1. Need to start a http server and listen to messages
    //  3. Add the necessary stuff from the previous instrumentation to start the server here

    private InetSocketAddress listenAddress;

    private List<InterceptorMessage> receivedMessages;

    public InterceptorServer(InetSocketAddress listenAddress) {
        this.listenAddress = listenAddress;
    }

    private void handleMessage() {

    }

    public List<InterceptorMessage> getReceivedMessages() {
        // TODO: thread safe access to the interceptor messages
        return null;
    }

}
