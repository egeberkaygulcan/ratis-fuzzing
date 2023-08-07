package org.apache.ratis.server.fuzzer.comm;

import java.util.HashMap;

import io.netty.handler.codec.http.*;

public class Route {
    public String path;
    public final HashMap<HttpMethod, MessageHandler> handlers;

    public Route(String path) {
        this.path = path;
        this.handlers = new HashMap<>();
    }

    public void get(MessageHandler handler) {
        addHandler(HttpMethod.GET, handler);
    }

    public void post(MessageHandler handler) {
        addHandler(HttpMethod.POST, handler);
    }

    public void addHandler(HttpMethod method, MessageHandler handler) {
        handlers.put(method, handler);
    }

    public FullHttpResponse handleRequest(FullHttpRequest req) {
        if (!this.path.equals(req.uri())) {
            return new DefaultFullHttpResponse(
                    req.protocolVersion(),
                    HttpResponseStatus.NOT_FOUND
            );
        }
        MessageHandler handler = handlers.get(req.method());
        if(handler == null) {
            return new DefaultFullHttpResponse(
                    req.protocolVersion(),
                    HttpResponseStatus.METHOD_NOT_ALLOWED
            );
        }
        return handler.handle(req);
    }
}
