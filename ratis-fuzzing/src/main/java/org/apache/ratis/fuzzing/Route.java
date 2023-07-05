package org.apache.ratis.server.impl.comm;

import java.util.HashMap;

import io.netty.handler.codec.http.*;

public class Route {
    public String path;
    public final HashMap<HttpMethod, Handler> handlers;

    public Route(String path) {
        this.path = path;
        this.handlers = new HashMap<>();
    }

    public void get(Handler handler) {
        addHandler(HttpMethod.GET, handler);
    }

    public void post(Handler handler) {
        addHandler(HttpMethod.POST, handler);
    }

    public void addHandler(HttpMethod method, Handler handler) {
        handlers.put(method, handler);
    }

    public FullHttpResponse handleRequest(FullHttpRequest req) {
        if (!this.path.equals(req.uri())) {
            return new DefaultFullHttpResponse(
                    req.protocolVersion(),
                    HttpResponseStatus.NOT_FOUND
            );
        }
        Handler handler = handlers.get(req.method());
        if(handler == null) {
            return new DefaultFullHttpResponse(
                    req.protocolVersion(),
                    HttpResponseStatus.METHOD_NOT_ALLOWED
            );
        }
        return handler.handle(req);
    }
}
