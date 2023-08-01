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
    private final String clientAddress = "127.0.0.1";
    private final int clientPort = 7075;

    private FuzzerCaller client;
    private NettyServer server;
    Channel serverChannel;
    private MessageHandler messageHandler;

    ConcurrentHashMap<Integer, Message> messageMap;
    ConcurrentHashMap<Integer, String> serverMap;
    AtomicInteger msgIdCounter;
    AtomicInteger serverIdCounter;
    int clientRequestQueue;
    Vector<String> shutdownQueue;
    Vector<String> restartQueue;
    boolean sendMetaData;
    boolean exit;

    public FuzzerClient() {
        this.client = new FuzzerCaller(fuzzerAddress + ":" + Integer.toString(fuzzerPort));
        this.messageHandler = new MessageHandler();
        this.messageMap = new ConcurrentHashMap<Integer, Message>();
        this.serverMap = new ConcurrentHashMap<Integer, String>();
        this.msgIdCounter = new AtomicInteger(0);
        this.serverIdCounter = new AtomicInteger(0);
        this.clientRequestQueue = 0;
        this.shutdownQueue = new Vector<String>();
        this.restartQueue = new Vector<String>();
        this.sendMetaData = false;
        this.exit = false;

        initServer();
    }

    private static class SingletonClient {
        private static final FuzzerClient INSTANCE = new FuzzerClient();
    }

    public static FuzzerClient getInstance(String caller) {
        return SingletonClient.INSTANCE;
    }

    private void initServer() {
        NettyRouter router = new NettyRouter();

        Route messagesRoute = new Route("/message");
        messagesRoute.post(messageHandler);
        router.addRoute(messagesRoute);

        Route replicasRoute = new Route("/replica");
        replicasRoute.post(messageHandler);
        router.addRoute(replicasRoute);

        this.server = new NettyServer(router);
        this.start();
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
            this.serverChannel = b.bind(clientAddress, clientPort).sync().channel();
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
        serverMap.put(Integer.valueOf(serverIdCounter.incrementAndGet()), id);

        JsonObject json = new JsonObject();
        json.addProperty("id", getServerId(id));
        json.addProperty("ready", true);
        json.addProperty("addr", clientAddress + ":" + Integer.toString(clientPort));
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

    public int getServerId(String id) {
        int ret = -1;
        for(Entry<Integer, String> entry: serverMap.entrySet()) {
            if(entry.getValue().equals(id)) {
                ret = entry.getKey().intValue();
              break;
            }
        }
        return ret;
    }

    public void interceptMessage(Message m) {
        if (m.getType() != "timeout") {
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

    public void scheduleClientReq() {
        this.clientRequestQueue++;
    }

    public void scheduleShutdown(int id) {
        this.shutdownQueue.add(this.serverMap.get(Integer.valueOf(id)));
    }

    public void scheduleRestart(int id) {
        this.restartQueue.add(this.serverMap.get(Integer.valueOf(id)));
    }

    public void scheduleMetadataRequest() {
        this.sendMetaData = true;
    }

    public void startExit() {
        System.out.println("EXIT SCHEDULED");
        this.exit = true;
    }

    public boolean metadataRequest() {
        boolean ret = this.sendMetaData;
        this.sendMetaData = false;
        return ret;
    }

    public int getClientRequests() {
        int ret = this.clientRequestQueue;
        this.clientRequestQueue = 0;
        return ret;
    }

    public Vector<String> getShutdown() {
        Vector<String> ret = (Vector<String>) this.shutdownQueue.clone();
        this.shutdownQueue.clear();
        return ret;
    }

    public Vector<String> getRestart() {
        Vector<String> ret = (Vector<String>) this.restartQueue.clone();
        this.restartQueue.clear();
        return ret;
    }

    public boolean exitProcess() {
        boolean ret = this.exit;
        // this.exit = false;
        return ret;
    }

    public boolean isControlledExecution() {
        return true;
    }
}
