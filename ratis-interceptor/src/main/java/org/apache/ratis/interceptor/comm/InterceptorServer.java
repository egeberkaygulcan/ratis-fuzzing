package org.apache.ratis.interceptor.comm;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.ratis.interceptor.InterceptorConfigKeys;
import org.apache.ratis.interceptor.comm.InterceptorClient.MessageHandler;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString.Output;
import org.apache.ratis.thirdparty.io.netty.buffer.ByteBuf;
import org.apache.ratis.thirdparty.io.netty.buffer.Unpooled;
import org.apache.ratis.thirdparty.io.netty.channel.ChannelFutureListener;
import org.apache.ratis.thirdparty.io.netty.channel.ChannelHandler;
import org.apache.ratis.thirdparty.io.netty.channel.ChannelHandlerContext;
import org.apache.ratis.thirdparty.io.netty.channel.EventLoopGroup;
import org.apache.ratis.thirdparty.io.netty.channel.SimpleChannelInboundHandler;
import org.apache.ratis.thirdparty.io.netty.handler.codec.http.DefaultFullHttpResponse;
import org.apache.ratis.thirdparty.io.netty.handler.codec.http.FullHttpRequest;
import org.apache.ratis.thirdparty.io.netty.handler.codec.http.FullHttpResponse;
import org.apache.ratis.thirdparty.io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.ratis.thirdparty.io.netty.handler.codec.http.HttpUtil;
import org.apache.ratis.thirdparty.io.netty.util.CharsetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.ratis.thirdparty.io.netty.handler.codec.http.HttpMethod;

import com.squareup.okhttp.Route;

import com.sun.net.httpserver.*;

import static org.apache.ratis.thirdparty.io.netty.handler.codec.http.HttpHeaderNames.*;
import static org.apache.ratis.thirdparty.io.netty.handler.codec.http.HttpHeaderValues.APPLICATION_JSON;
import static org.apache.ratis.thirdparty.io.netty.handler.codec.http.HttpHeaderValues.CLOSE;
import static org.apache.ratis.thirdparty.io.netty.handler.codec.http.HttpHeaderValues.KEEP_ALIVE;

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
        start(NanoHTTPD.SOCKET_READ_TIMEOUT, false);

    }

    @Override
    public Response serve(IHTTPSession session) {
        if (session.getMethod() == Method.POST) {
            try {
                session.parseBody(new HashMap<>());
                String requestBody = session.getQueryParameterString();
                // TODO: Parse body and add to receivedMessages
                return newFixedLengthResponse("Request body + " + requestBody);
            } catch (IOException | ResponseException e) {
                LOG.error("Exception while receiving POST.", e);
            }
        }

        return newFixedLengthResponse(Response.Status.NOT_FOUND, MIME_PLAINTEXT, 
            "The requested resource does not exist");
    }

    public List<InterceptorMessage> getReceivedMessages() {
        // TODO: thread safe access to the interceptor messages
        return null;
    }

        
}
