package org.apache.ratis.server.fuzzer;

import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.ratis.server.fuzzer.comm.FuzzerCaller;
import org.apache.ratis.server.fuzzer.comm.GsonHelper;
import org.apache.ratis.server.fuzzer.comm.JsonMessage;
import org.apache.ratis.server.fuzzer.comm.MessageHandler;
import org.apache.ratis.server.fuzzer.comm.NettyRouter;
import org.apache.ratis.server.fuzzer.comm.NettyServer;
import org.apache.ratis.server.fuzzer.comm.Route;
import org.apache.ratis.server.fuzzer.events.Event;
import org.apache.ratis.server.fuzzer.messages.Message;

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
    private int serverClientPort = 7080;

    private FuzzerCaller client;
    private ReentrantLock clientLock;
    private NettyServer server;
    Channel serverChannel;
    private MessageHandler messageHandler;

    ConcurrentHashMap<String, Message> messageMap;
    AtomicInteger msgIdCounter;

    // private RaftPeerId leaderId;

    private boolean shutdownFlag;
    private boolean crashFlag;
    private boolean restartFlag;
    private boolean electionFlag;

    private String serverId;

    public FuzzerClient() {
        this.client = new FuzzerCaller(fuzzerAddress + ":" + Integer.toString(fuzzerPort));
        this.clientLock = new ReentrantLock(true);
        this.messageHandler = new MessageHandler();
        this.messageMap = new ConcurrentHashMap<String, Message>();
        this.msgIdCounter = new AtomicInteger(0);
        this.shutdownFlag = false;
        this.crashFlag = false;
        this.restartFlag = false;
        this.electionFlag = false;
        // this.leaderId = null;
    }

    private static class SingletonClient {
        private static final FuzzerClient INSTANCE = new FuzzerClient();
    }

    public static FuzzerClient getInstance() {
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

        Route eventsRoute = new Route("/event");
        eventsRoute.post(messageHandler);
        router.addRoute(eventsRoute);

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
        this.serverId = id;

        JsonObject json = new JsonObject();
        json.addProperty("id", id);
        json.addProperty("ready", true);
        json.addProperty("addr", serverClientAddress + ":" + Integer.toString(serverClientPort));
        json.addProperty("info", "");

        Gson gson = GsonHelper.gson;
        try {
            this.clientLock.lock();
            client.sendReplica(gson.toJson(json));
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            this.clientLock.unlock();
        }
    }

    public String generateId() {
        return this.serverId + "_" + Integer.valueOf(this.msgIdCounter.getAndIncrement()).toString();
    }

    public void interceptMessage(Message m) {
        try {
            this.clientLock.lock();
            client.sendMessage(m.toJsonString());
        } catch (Exception e) {
            // e.printStackTrace();
        } finally {
            this.clientLock.unlock();
        }
        messageMap.put(m.getId(), m);
    }

    public void sendEvent(Event e) {
        try {
            this.clientLock.lock();
            client.sendEvent(e.toJsonString());
        } catch (Exception ex) {
            // ex.printStackTrace();
        } finally {
            this.clientLock.unlock();
        }
    }

    public void getAndExecuteMessages(){
        if (this.shutdownFlag) 
            return;
        Vector<JsonMessage> messages = this.messageHandler.getMessages();
        for(JsonMessage msg : messages) {
            if (msg.id == null) {
                System.out.println("Null message id.");
                continue;
            }
            Message msgImpl = this.messageMap.remove(msg.id);
            if (msgImpl != null)
                try {
                    msgImpl.invoke();
                } catch (Exception e) {
                    System.out.println(msgImpl.getType());
                    e.printStackTrace();
                }
        }
    }

    // public void askLeaderId() {
    //     this.sendEvent(new AskLeaderEvent());
    // }

    // public void setLeaderId(RaftPeerId id) {

    //     this.leaderId = id;
    // }

    // public RaftPeerId getLeaderId() {
    //     return this.leaderId;
    // }

    public void startShutdown() {
        this.shutdownFlag = true;
    }

    public void setElection(boolean b) {
        this.electionFlag = b;
    }

    public boolean getElection() {
        return this.electionFlag;
    }

    public void startCrash() {
        this.crashFlag = true;
    }

    public void startRestart() {
        this.restartFlag = true;
    }

    public boolean shouldShutdown() {
        return this.shutdownFlag;
    }

    public void crashed() {
        this.crashFlag = false;
    }

    public boolean shouldCrash() {
        return this.crashFlag;
    }

    public void restarted() {
        this.restartFlag = false;
    }

    public boolean shouldRestart() {
        return this.restartFlag;
    }

    public boolean isControlledExecution() {
        return true;
    }

    public void setServerClientPort(int port) {
        this.serverClientPort = port;
    }

    public void setServerId(String id) { 
        this.serverId = id;
    }

    public String getServerId() {
        return this.serverId;
    }
    
    public void clearMessageQueue() {
        this.messageMap.clear();
        this.messageHandler.clearMessages();
    }
}