package org.apache.ratis.server.impl.comm;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.*;

import static io.netty.handler.codec.http.HttpHeaderNames.*;
import static io.netty.handler.codec.http.HttpHeaderValues.CLOSE;
import static io.netty.handler.codec.http.HttpHeaderValues.KEEP_ALIVE;

public class NettyServer extends SimpleChannelInboundHandler<Object> {
    private final NettyRouter router;

    public NettyServer(NettyRouter router) {
        this.router = router;
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        ctx.close();
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof  FullHttpRequest) {
            FullHttpRequest req = (FullHttpRequest) msg;
            FullHttpResponse res;
            try {
                res = this.router.handleRequest(req);
            } catch (Exception e) {
                res = new DefaultFullHttpResponse(req.protocolVersion(),
                        HttpResponseStatus.INTERNAL_SERVER_ERROR);
            }
            res.headers().set(CONTENT_TYPE, "text/plain; charset=UTF-8");
            if (HttpUtil.isKeepAlive(req)) {
                res.headers().set(CONTENT_LENGTH, res.content().readableBytes());
                res.headers().set(CONNECTION, KEEP_ALIVE);
                ctx.writeAndFlush(res);
            } else {
                res.headers().set(CONNECTION, CLOSE);
                ctx.writeAndFlush(res).addListener(ChannelFutureListener.CLOSE);
            }
        }
    }
}
