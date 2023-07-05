package org.apache.ratis.server.impl;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.apache.ratis.proto.RaftProtos.AppendEntriesReplyProto;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.thirdparty.com.google.gson.JsonElement;
import org.apache.ratis.thirdparty.org.checkerframework.checker.units.qual.radians;

import com.google.gson.JsonObject;

import org.apache.ratis.server.impl.comm.*;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

import java.io.IOException;
import java.util.HashMap;
import java.util.Vector;

// TODO - Rename and modify for Python fuzzer
public class NetrixClient extends Thread {
    NettyServer server;
    NetrixCaller client;
    Timer timer;
    NetrixClientConfig netrixClientConfig;
    MessageHandler messageHandler;

    Channel serverChannel;

    Counter counter;

    private boolean isRandomExecution;
    private boolean isControlledTimeout;
    private HashMap<String, Integer> idMap;
    private Counter idCounter;

    public NetrixClient(NetrixClientConfig c) {
        this.counter = new Counter();
        this.idCounter = new Counter();
        this.netrixClientConfig = c;
        this.timer = new Timer();
        this.client = new NetrixCaller(c);
        this.messageHandler = new MessageHandler(this.client);
        this.isRandomExecution = true;
        this.isControlledTimeout = false;
        this.idMap = new HashMap<>();
        initServer();

        try {
            this.client.register();
        } catch (IOException ignored) {

        }
    }

    @Override
    public void run() {
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
                .childOption(ChannelOption.SO_KEEPALIVE, true);;
            this.serverChannel = b.bind(netrixClientConfig.clientServerAddr, netrixClientConfig.clientServerPort).sync().channel();
            this.serverChannel.closeFuture().sync();
        } catch (Exception ignored) {
        } finally {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
        }
    }

    public void stopClient() {
        try {
            this.serverChannel.close();
        } catch (Exception ignored) {

        }
    }

    private void initServer() {
        NettyRouter router = new NettyRouter();

        Route messagesRoute = new Route("/message");
        messagesRoute.post(messageHandler);
        router.addRoute(messagesRoute);

        this.server = new NettyServer(router);
    }

    private String nextID(String from, String to) {
        counter.incr();
        return String.format("%s_%s_%d", from, to, counter.getValue());
    }

    public Vector<Message> getMessages() {
        return messageHandler.getMessages();
    }

    public void sendMessage(Message message) throws IOException {
        String messageID = nextID(netrixClientConfig.replicaID, message.getTo());
        message.setId(messageID);

        client.sendMessage(message);
    }

    public boolean isRandomExecution() {
        return this.isRandomExecution;
    }

    public boolean isControlledTimeout() {
        return this.isControlledTimeout;
    }

    public void sendTimeout(RaftPeerId id, FollowerState fs) {}

    public void sendVoteRequest(RaftPeerId sender, RaftPeerId receiver, boolean isPreVote, long term, TermIndex lastEntry) {
        JsonObject jo = new JsonObject();
        String type = "request_vote_request";
        jo.addProperty("type", type);
        jo.addProperty("prevote", isPreVote);
        jo.addProperty("term", term);
        System.out.println("Sender: " + sender.toString());
        jo.addProperty("candidate_id", this.idMap.get(sender.toString()));
        jo.addProperty("last_log_idx", lastEntry.getIndex());
        jo.addProperty("last_log_term", lastEntry.getTerm());

        byte[] data = jo.toString().getBytes(); 
        System.out.println(this.idMap);
        System.out.println(receiver.toString());
        Message msg = new Message(Integer.toString(this.idMap.get(receiver.toString())), type, data);
        try {
            this.client.sendMessage(msg);
        } catch (Exception e) {
            System.out.println("Could not send VoteRequest");
            e.printStackTrace();
        }
    }

    public void sendVoteRequestReply(RaftPeerId sender, RaftPeerId receiver, boolean isPreVote, long currentTerm, long candidateTerm, boolean voteGranted) {
        JsonObject jo = new JsonObject();
        String type = "request_vote_response";
        jo.addProperty("type", type);
        jo.addProperty("prevote", isPreVote);
        jo.addProperty("term", currentTerm);
        jo.addProperty("request_term", candidateTerm);
        jo.addProperty("vote_granted", voteGranted);

        byte[] data = jo.toString().getBytes(); 

        Message msg = new Message(Integer.toString(this.idMap.get(receiver.toString())), type, data);
        try {
            this.client.sendMessage(msg);
        } catch (Exception e) {
            System.out.println("Could not send VoteRequest");
            e.printStackTrace();
        }
    }

    public void sendAppendEntries(RaftPeerId sender, RaftPeerId receiver, long term, long leaderCommit, List<LogEntryProto> entryList, RaftLog log) {
        JsonObject jo = new JsonObject();
        String type = "append_entries_request";
        jo.addProperty("type", type);
        jo.addProperty("leader_id", this.idMap.get(sender.toString()));
        jo.addProperty("term", term); 
        jo.addProperty("prev_log_idx", log.getLastCommittedIndex());
        jo.addProperty("prev_log_term", log.getLastEntryTermIndex().getTerm());
        jo.addProperty("leader_commit", leaderCommit); 
        jo.addProperty("msg_id", log.toString());

        JsonObject entries = new JsonObject();
        for (int i = 0; i < entryList.size(); i++) {
            JsonObject entry = new JsonObject();
            entry.addProperty("term", entryList.get(i).getTerm());
            entry.addProperty("id", entryList.get(i).getIndex());
            entry.addProperty("session", i);
            entry.addProperty("type", entryList.get(i).getLogEntryBodyCase().getClass().toString());
            entry.addProperty("data", entryList.get(i).getLogEntryBodyCase().toString());

            entries.addProperty(Integer.toString(i), entry.toString());
        }
        jo.addProperty("entries", entries.toString());


        byte[] data = jo.toString().getBytes(); 

        Message msg = new Message(Integer.toString(this.idMap.get(receiver.toString())), type, data);
        try {
            this.client.sendMessage(msg);
        } catch (Exception e) {
            System.out.println("Could not send VoteRequest");
            e.printStackTrace();
        }
    }

    public void sendAppendEntriesReply(RaftPeerId sender, RaftPeerId receiver, long term, boolean success, CompletableFuture<AppendEntriesReplyProto> replyFuture) {
        try {
            AppendEntriesReplyProto reply = replyFuture.get();
            long currentIndex = reply.getMatchIndex();
            String msgId = reply.toString();
        } catch (Exception e) {
            System.out.println("Netrix Client send AppendEntriesReply");
            e.printStackTrace();
        }

    }

    // TODO - Single message listener for all types of actions


    // TODO - Insert actions received from Netrix to the newly implemented server message queues

    public void setReady() throws IOException {
        this.client.setReady();
    }

    public void unsetReady() throws IOException {
        this.client.unsetReady();
    }

    public void setRandomExecution(boolean b) {
        System.out.println("Random execution is set to " + Boolean.toString(b));
        this.isRandomExecution = b;
    }

    public void setControlledTimeout(boolean b) {
        this.isControlledTimeout = b;
    }

    // public void sendEvent(String type, HashMap<String, String> params) throws IOException{
    //     Event event = new Event(type, params);
    //     event.setReplicaID(netrixClientConfig.replicaID);
    //     client.sendEvent(event);
    // }

    // public void sendEvent(Event event) throws IOException {
    //     event.setReplicaID(netrixClientConfig.replicaID);
    //     client.sendEvent(event);
    // }

    // public void startTimeout(Timeout timeout) {
    //     if(timer.addTimeout(timeout)) {
    //         HashMap<String, String> params = new HashMap<String, String>();
    //         params.put("type", timeout.key());
    //         params.put("duration", timeout.getDuration().toString());
    //         Event event = new Event("TimeoutStart", params);
    //     }
    // }
}