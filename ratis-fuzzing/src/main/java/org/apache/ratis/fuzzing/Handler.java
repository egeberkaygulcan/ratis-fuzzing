package org.apache.ratis.server.impl.comm;

import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;

public interface Handler {
    FullHttpResponse handle(FullHttpRequest request);
}
