package org.apache.ratis.server.fuzzer;

import java.util.Vector;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.ratis.server.fuzzer.comm.FuzzerCaller;
import org.apache.ratis.server.fuzzer.comm.GsonHelper;
import org.apache.ratis.server.fuzzer.comm.JsonMessage;
import org.apache.ratis.server.fuzzer.comm.MessageHandler;
import org.apache.ratis.server.fuzzer.comm.NettyRouter;
import org.apache.ratis.server.fuzzer.comm.NettyServer;
import org.apache.ratis.server.fuzzer.comm.Route;
import org.apache.ratis.server.fuzzer.messages.Message;
import org.apache.ratis.server.impl.RaftServerImpl;
import org.apache.ratis.server.impl.RaftServerProxy;
import org.apache.ratis.statemachine.StateMachine;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import io.netty.handler.codec.http.*;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

public class FuzzerClient extends Thread{

    private final String fuzzerAddress = "127.0.0.1";
    private final int fuzzerPort = 7074;
    private final String serverClientAddress = "127.0.0.1";
    private int serverClientPort;

    private FuzzerCaller client;
    private NettyServer server;
    Channel serverChannel;
    private MessageHandler messageHandler;

    ConcurrentHashMap<Integer, Message> messageMap;
    AtomicInteger msgIdCounter;

    RaftServerImpl impl;
    RaftServerProxy proxy;
    StateMachine stateMachine;
    

    public FuzzerClient() {
        this.client = new FuzzerCaller(fuzzerAddress + ":" + Integer.toString(fuzzerPort));
        this.messageHandler = new MessageHandler();
        this.messageMap = new ConcurrentHashMap<Integer, Message>();
        this.msgIdCounter = new AtomicInteger(0);
    }

    private static class SingletonClient {
        private static final FuzzerClient INSTANCE = new FuzzerClient();
    }

    public static FuzzerClient getInstance(String caller) {
        return SingletonClient.INSTANCE;
    }

    public void initServer() {
        NettyRouter router = new NettyRouter();

        Route messagesRoute = new Route("/message");
        messagesRoute.post(messageHandler);
        router.addRoute(messagesRoute);

        Route replicasRoute = new Route("/replica");
        replicasRoute.post(messageHandler);
        router.addRoute(replicasRoute);

        this.server = new NettyServer(router);
        SingletonClient.INSTANCE.start();
    }

    @Override
    public void run() {
        System.out.println("RUNNING FUZZERCLIENT");
        EventLoopGroup bossGroup = new NioEventLoopGroup();
        EventLoopGroup workerGroup = new NioEventLoopGroup();

        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .handler(new LoggingHandler(LogLevel.INFO))
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) throws Exception {
                        ChannelPipeline p = ch.pipeline();
                        p.addLast(new HttpRequestDecoder());
                        p.addLast(new HttpObjectAggregator(1048576));
                        p.addLast(new HttpResponseEncoder());
                        p.addLast(server);
                    }
                })
                .childOption(ChannelOption.SO_KEEPALIVE, true);
            this.serverChannel = b.bind(serverClientAddress, serverClientPort).sync().channel();
            this.serverChannel.closeFuture().sync();
        } catch (Exception e) {
            System.out.println("-------- EXCEPTION --------");
            e.printStackTrace();
        } finally {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
        }
    }

    public void registerServer(String id) {

        JsonObject json = new JsonObject();
        json.addProperty("id", id);
        json.addProperty("ready", true);
        json.addProperty("addr", serverClientAddress + ":" + Integer.toString(serverClientPort));
        json.addProperty("info", "");

        Gson gson = GsonHelper.gson;
        try {
            client.sendReplica(gson.toJson(json));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public Integer generateId() {
        return Integer.valueOf(this.msgIdCounter.getAndIncrement());
    }

    public void interceptMessage(Message m) {
        if (!m.getType().equals("timeout")){
            System.out.println("Sending message to: " + fuzzerAddress + ":" + Integer.toString(fuzzerPort));
            try {
                client.sendMessage(m.toJsonString());
            } catch (Exception e) {
                e.printStackTrace();
            }
            messageMap.put(m.getId(), m);
        }
    }

    public void getAndExecuteMessages(){
        Vector<JsonMessage> messages = this.messageHandler.getMessages();
        for(JsonMessage msg : messages) {
            if (msg.id == null)
                continue;
            Message msgImpl = this.messageMap.remove(Integer.valueOf(msg.id));
            if (msgImpl != null)
                try {
                    msgImpl.invoke();
                } catch (Exception e) {
                    System.out.println(msgImpl.getType());
                    e.printStackTrace();
                }
        }
    }

    public void startShutdown() {
        System.out.println("------ Shutdown started. ------");
        this.proxy.close();

        try {
            this.stateMachine.takeSnapshot();
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            this.stateMachine.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("------ Shutdown complete. ------");
    }

    public boolean isControlledExecution() {
        return true;
    }

    public void setServerClientPort(int port) {
        this.serverClientPort = port;
    }

    public void setRaftImpl(RaftServerImpl impl) {
        this.impl = impl;
    }

    public void setRaftProxy(RaftServerProxy proxy) {
        this.proxy = proxy;
    }

    public void setStateMachine(StateMachine stateMachine) {
        this.stateMachine = stateMachine;
    }
}
