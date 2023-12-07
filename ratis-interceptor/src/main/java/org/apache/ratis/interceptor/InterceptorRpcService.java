package org.apache.ratis.interceptor;

import org.apache.ratis.interceptor.comm.InterceptorClient;
import org.apache.ratis.interceptor.comm.InterceptorMessage;
import org.apache.ratis.interceptor.comm.InterceptorMessageUtils;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerRpcWithProxy;
import org.apache.ratis.thirdparty.io.netty.bootstrap.ServerBootstrap;
import org.apache.ratis.thirdparty.io.netty.buffer.ByteBuf;
import org.apache.ratis.thirdparty.io.netty.buffer.Unpooled;
import org.apache.ratis.thirdparty.io.netty.channel.ChannelFuture;
import org.apache.ratis.thirdparty.io.netty.channel.ChannelFutureListener;
import org.apache.ratis.thirdparty.io.netty.channel.ChannelHandlerContext;
import org.apache.ratis.thirdparty.io.netty.channel.ChannelInitializer;
import org.apache.ratis.thirdparty.io.netty.channel.ChannelPipeline;
import org.apache.ratis.thirdparty.io.netty.channel.EventLoopGroup;
import org.apache.ratis.thirdparty.io.netty.channel.SimpleChannelInboundHandler;
import org.apache.ratis.thirdparty.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.ratis.thirdparty.io.netty.channel.socket.SocketChannel;
import org.apache.ratis.thirdparty.io.netty.channel.socket.nio.NioServerSocketChannel;
import org.apache.ratis.thirdparty.io.netty.handler.codec.http.DefaultFullHttpResponse;
import org.apache.ratis.thirdparty.io.netty.handler.codec.http.FullHttpRequest;
import org.apache.ratis.thirdparty.io.netty.handler.codec.http.FullHttpResponse;
import org.apache.ratis.thirdparty.io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.ratis.thirdparty.io.netty.handler.codec.http.HttpUtil;
import org.apache.ratis.thirdparty.io.netty.handler.codec.http.HttpObjectAggregator;
import org.apache.ratis.thirdparty.io.netty.handler.codec.http.HttpRequestDecoder;
import org.apache.ratis.thirdparty.io.netty.handler.codec.http.HttpResponseEncoder;
import org.apache.ratis.thirdparty.io.netty.handler.logging.LogLevel;
import org.apache.ratis.thirdparty.io.netty.handler.logging.LoggingHandler;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.MemoizedSupplier;
import org.apache.ratis.util.TimeDuration;
import org.apache.ratis.proto.RaftProtos.*;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

import static org.apache.ratis.thirdparty.io.netty.handler.codec.http.HttpHeaderNames.*;
import static org.apache.ratis.thirdparty.io.netty.handler.codec.http.HttpHeaderValues.*;
import static org.apache.ratis.thirdparty.io.netty.handler.codec.http.HttpHeaderValues.KEEP_ALIVE;

public class InterceptorRpcService extends RaftServerRpcWithProxy<InterceptorRpcProxy, InterceptorRpcProxy.PeerMap> {
    public static final Logger LOG = LoggerFactory.getLogger(InterceptorRpcService.class);

    private final RaftServer raftServer;
    private final InetSocketAddress iListenerAddress;
    private final InetSocketAddress listenerAddress;
    private final InetSocketAddress serverAddress;
    private final boolean intercept;
    private final InterceptorClient iClient;
    private final MemoizedSupplier<ChannelFuture> channel;

    class InboundHandler extends SimpleChannelInboundHandler<Object> {
        @Override
        public void channelReadComplete(ChannelHandlerContext ctx) {
            ctx.flush();
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) { ctx.close(); }

        @Override
        public void channelRead0(ChannelHandlerContext ctx, Object msg) {
            if (msg instanceof  FullHttpRequest) {
                LOG.info("Received new message.");
                FullHttpRequest req = (FullHttpRequest) msg;
                FullHttpResponse res;
                try {
                    ByteBuf content = req.content();
                    if(content == null || content.readableBytes() <= 0){
                        throw new IOException("empty request");
                    }
    
                    if(!(req.headers().get(CONTENT_TYPE).equals(APPLICATION_JSON.toString()) || req.headers().get(CONTENT_TYPE).equals("application/json; charset=utf-8"))) {
                        LOG.info("Throwing IOException.");
                        throw new IOException("not a json request");
                    }
                    LOG.info("Building message with Json string.");
                    InterceptorMessage requestMessage = new InterceptorMessage.Builder().buildWithJsonString(content.toString(StandardCharsets.UTF_8));
                    InterceptorMessage replyMessage = handle(requestMessage);
                    byte[] responseContent = replyMessage.toJsonString().getBytes(StandardCharsets.UTF_8);
                    res = new DefaultFullHttpResponse(req.protocolVersion(), HttpResponseStatus.OK, Unpooled.copiedBuffer(responseContent));
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

    public InterceptorRpcService(RaftServer server) {
        super(server::getId, id -> new InterceptorRpcProxy.PeerMap(id.toString(), server.getProperties()));
        LOG.info("InterceptorRpcService started.");
        this.raftServer = server;
        
        final String iLhost = InterceptorConfigKeys.InterceptorListener.host(server.getProperties());
        final int iLport = InterceptorConfigKeys.InterceptorListener.port(server.getProperties());
        this.iListenerAddress = new InetSocketAddress(iLhost, iLport);

        final String lhost = InterceptorConfigKeys.Listener.host(server.getProperties());
        final int lport = InterceptorConfigKeys.Listener.port(server.getProperties());
        this.listenerAddress = new InetSocketAddress(lhost, lport);

        final String shost = InterceptorConfigKeys.Server.host(server.getProperties());
        final int sport = InterceptorConfigKeys.Server.port(server.getProperties());
        this.serverAddress = new InetSocketAddress(shost, sport);

        this.intercept = InterceptorConfigKeys.enabled(server.getProperties());
        TimeDuration replyWaitTimeout = InterceptorConfigKeys.replyWaitTimeout(server.getProperties());
        this.iClient = this.intercept ? new InterceptorClient(server, this.serverAddress, this.iListenerAddress, replyWaitTimeout, this::handle) : null;

        final ChannelInitializer<SocketChannel> initializer
            = new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) {
                ChannelPipeline p = ch.pipeline();
                p.addLast(new HttpRequestDecoder());
                p.addLast(new HttpObjectAggregator(1048576));
                p.addLast(new HttpResponseEncoder());
                p.addLast(new InboundHandler());
            }
        };


        EventLoopGroup bossGroup = new NioEventLoopGroup();
        EventLoopGroup workerGroup = new NioEventLoopGroup();

        this.channel = JavaUtils.memoize(() -> new ServerBootstrap()
            .group(bossGroup, workerGroup)
            .channel(NioServerSocketChannel.class)
            .handler(new LoggingHandler(LogLevel.INFO))
            .childHandler(initializer)
            .bind(this.listenerAddress));
    }

    @Override
    public SupportedRpcType getRpcType() { return SupportedRpcType.INTERCEPTOR; }

    @Override
    public void startImpl() throws IOException {
        if(this.intercept) {
            this.iClient.start();
        }
        try {
            channel.get().syncUninterruptibly();
        } catch(Exception t) {
            throw new IOException(getId() + ": Failed to start " + JavaUtils.getClassSimpleName(getClass()), t);
        }
    }

    @Override
    public void closeImpl() throws IOException {
        if(this.intercept) {
            this.iClient.stop();
        }
    }

    @Override
    public InetSocketAddress getInetSocketAddress() {
        return listenerAddress;
    }


    InterceptorMessage handle(InterceptorMessage message) throws IOException{
        InterceptorMessageUtils.MessageType messageType = InterceptorMessageUtils.MessageType.fromString(message.getType());
        LOG.info("Handling message of type " + messageType.toString());
        switch (messageType) {
            case RequestVoteRequest:
                RequestVoteReplyProto reply = this.raftServer.requestVote(message.toRequestVoteRequest());
                return new InterceptorMessage.Builder().setRequestVoteReply(reply).build();
            case AppendEntriesRequest:
                AppendEntriesReplyProto aEReply = this.raftServer.appendEntries(message.toAppendEntriesRequest());
                return new InterceptorMessage.Builder().setAppendEntriesReply(aEReply).build();
            case InstallSnapshotRequest:
                InstallSnapshotReplyProto iSReply = this.raftServer.installSnapshot(message.toInstallSnapshotRequest());
                return new InterceptorMessage.Builder().setInstallSnapshotReply(iSReply).build();
            case StartLeaderElectionRequest:
                StartLeaderElectionReplyProto sLEReply = this.raftServer.startLeaderElection(message.toStartLeaderElectionRequest());
                return new InterceptorMessage.Builder().setStartLeaderElectionReply(sLEReply).build();
            case RaftClientRequest:
                RaftClientReply RCReply = this.raftServer.submitClientRequest(message.toRaftClientRequest());
                return new InterceptorMessage.Builder().setRaftClientReply(RCReply).build();
            default:
                break;
        }
        return null;
    }

    @Override
    public RequestVoteReplyProto requestVote(RequestVoteRequestProto request) throws IOException {
        LOG.info("RequestVoteRequest from " + request.getServerRequest().getRequestorId().toStringUtf8() + " to " + request.getServerRequest().getReplyId().toStringUtf8());
        InterceptorMessage.Builder iMessageBuilder = new InterceptorMessage.Builder()
                .setRequestVoteRequest(request);

        if(this.intercept) {
            iMessageBuilder.setRequestId(iClient.getNewRequestId());
            InterceptorMessage message =  iClient.sendMessage(iMessageBuilder);
            return message.toRequestVoteReply();
        }
        final RaftPeerId id = RaftPeerId.valueOf(request.getServerRequest().getReplyId());
        InterceptorRpcProxy proxy = getProxies().getProxy(id);
        LOG.info("Proxy address: " + proxy.getPeerAddress());
        InterceptorMessage reply = proxy.send(iMessageBuilder.build());

        if (reply == null) {
            throw new IOException("Received null reply.");
        }
        
        return reply.toRequestVoteReply();
    }

    @Override
    public AppendEntriesReplyProto appendEntries(AppendEntriesRequestProto request) throws IOException {
        LOG.info("AppendEntriesRequest from " + request.getServerRequest().getRequestorId().toStringUtf8() + " to " + request.getServerRequest().getReplyId().toStringUtf8());
        InterceptorMessage.Builder iMessageBuilder = new InterceptorMessage.Builder()
                .setAppendEntriesRequest(request);

        if(this.intercept) {
            iMessageBuilder.setRequestId(iClient.getNewRequestId());
            InterceptorMessage message = iClient.sendMessage(iMessageBuilder);
            return message.toAppendEntriesReply();
        }

        final RaftPeerId id = RaftPeerId.valueOf(request.getServerRequest().getReplyId());
        InterceptorRpcProxy proxy = getProxies().getProxy(id);
        InterceptorMessage reply = proxy.send(iMessageBuilder.build());

        if (reply == null) {
            throw new IOException("Received null reply.");
        }

        return reply.toAppendEntriesReply();
    }

    @Override
    public InstallSnapshotReplyProto installSnapshot(InstallSnapshotRequestProto request) throws IOException {
        LOG.info("InstallSnapshotRequest from " + request.getServerRequest().getRequestorId().toStringUtf8() + " to " + request.getServerRequest().getReplyId().toStringUtf8());
        InterceptorMessage.Builder iMessageBuilder = new InterceptorMessage.Builder()
                .setInstallSnapshotRequest(request);

        if(this.intercept) {
            iMessageBuilder.setRequestId(iClient.getNewRequestId());
            InterceptorMessage message = iClient.sendMessage(iMessageBuilder);
            return message.toInstallSnapshotReply();
        }

        final RaftPeerId id = RaftPeerId.valueOf(request.getServerRequest().getReplyId());
        InterceptorRpcProxy proxy = getProxies().getProxy(id);
        InterceptorMessage reply = proxy.send(iMessageBuilder.build());

        if (reply == null) {
            throw new IOException("Received null reply.");
        }

        return reply.toInstallSnapshotReply();
    }

    @Override
    public StartLeaderElectionReplyProto startLeaderElection(StartLeaderElectionRequestProto request) throws IOException {
        LOG.info("StartLeaderElectionRequest from " + request.getServerRequest().getRequestorId().toStringUtf8() + " to " + request.getServerRequest().getReplyId().toStringUtf8());
        InterceptorMessage.Builder iMessageBuilder = new InterceptorMessage.Builder()
                .setStartLeaderElectionRequest(request);   

        if(this.intercept) {
            iMessageBuilder.setRequestId(iClient.getNewRequestId());
            InterceptorMessage message = iClient.sendMessage(iMessageBuilder);
            return message.toStartLeaderElectionReply();
        }

        final RaftPeerId id = RaftPeerId.valueOf(request.getServerRequest().getReplyId());
        InterceptorRpcProxy proxy = getProxies().getProxy(id);
        InterceptorMessage reply = proxy.send(iMessageBuilder.build());

        if (reply == null) {
            throw new IOException("Received null reply.");
        }

        return reply.toStartLeaderElectionReply();
    }
}
