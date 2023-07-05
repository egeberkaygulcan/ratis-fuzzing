package org.apache.ratis.server.impl.comm;

import io.netty.handler.codec.http.*;

import java.util.HashMap;

public class NettyRouter {
    private final HashMap<String, Route> routes;

    public NettyRouter() {
        this.routes = new HashMap<>();
    }

    public void addRoute(Route route) {
        this.routes.put(route.path, route);
    }

    public FullHttpResponse handleRequest(FullHttpRequest req) {
        Route route = this.routes.get(req.uri());
        if(route == null) {
            return new DefaultFullHttpResponse(
                    req.protocolVersion(),
                    HttpResponseStatus.NOT_FOUND
            );
        }
        return route.handleRequest(req);
    }
}