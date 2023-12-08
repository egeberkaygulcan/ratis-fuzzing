package org.apache.ratis.interceptor.comm;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import fi.iki.elonen.NanoHTTPD;
public class InterceptorServer extends NanoHTTPD {
        
    // }
    // TODO:
    //  1. Need to start a http server and listen to messages
    //  3. Add the necessary stuff from the previous instrumentation to start the server here

    Logger LOG = LoggerFactory.getLogger(InterceptorServer.class);

    private InetSocketAddress listenAddress;

    private List<InterceptorMessage> receivedMessages;

    public InterceptorServer(InetSocketAddress listenAddress) throws IOException {
        super(listenAddress.getPort());
        this.listenAddress = listenAddress;
        this.receivedMessages = new CopyOnWriteArrayList<InterceptorMessage>();
    }

    public void startServer() throws IOException {
        start(NanoHTTPD.SOCKET_READ_TIMEOUT, false); // TODO: Is this blocking?
    }

    @Override
    public Response serve(IHTTPSession session) {
        if (session.getMethod() == Method.POST) {
            try {
                session.parseBody(new HashMap<>());
                String requestBody = session.getQueryParameterString();
                InterceptorMessage.Builder b = new InterceptorMessage.Builder();
                InterceptorMessage message = b.buildWithJsonString(requestBody);
                this.receivedMessages.add(message);
                return newFixedLengthResponse("Ok\n");
            } catch (IOException | ResponseException e) {
                LOG.error("Exception while receiving POST.", e);
            }
        }

        return newFixedLengthResponse(Response.Status.NOT_FOUND, MIME_PLAINTEXT, 
            "The requested resource does not exist");
    }

    public List<InterceptorMessage> getReceivedMessages() {
        List<InterceptorMessage> ret = new CopyOnWriteArrayList<InterceptorMessage>(this.receivedMessages);
        this.receivedMessages.clear();
        return ret;
    }
}
