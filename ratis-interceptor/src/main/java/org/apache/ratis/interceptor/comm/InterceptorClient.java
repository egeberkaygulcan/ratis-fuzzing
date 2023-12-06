package org.apache.ratis.interceptor.comm;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.squareup.okhttp.*;

import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.exceptions.TimeoutIOException;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.util.IOUtils;
import org.apache.ratis.util.TimeDuration;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class InterceptorClient {
    // TODO
    //  [ ] Need to initiate a connection pool to the interceptor server
    //  [x] Need to define an message interface to communicate with the interceptor server
    //  [ ] Need to figure out how to start the listener server (own the thread? extend the thread interface?)
    //  [x] Need to tag and keep track of requests and implement a future interface

    @FunctionalInterface
    public static interface MessageHandler {
        InterceptorMessage apply(InterceptorMessage interceptorMessage) throws IOException;
    }

    private RaftServer raftServer;
    private InetSocketAddress interceptorAddress;
    private InetSocketAddress listenAddress;
    private InterceptorServer listenServer;
    private TimeDuration replyWaitTime;
    private OkHttpClient client = new OkHttpClient();
    private MessagePollingThread pollingThread; 
    private AtomicInteger counter;
    private Random random;

    public InterceptorClient(
        RaftServer raftServer, 
        InetSocketAddress interceptorAddress, 
        InetSocketAddress listenAddress, 
        TimeDuration replyWaitTime,
        MessageHandler messageHandler
    ) {
        this.raftServer = raftServer;
        this.interceptorAddress = interceptorAddress;
        this.listenAddress = listenAddress;
        this.replyWaitTime = replyWaitTime;

        try {
            this.listenServer = new InterceptorServer(listenAddress);
        } catch (Exception e) {
            // TODO: handle exception
        }
        this.pollingThread = new MessagePollingThread(this.listenServer, messageHandler);
        this.counter = new AtomicInteger();
        this.random = new Random((long) this.listenAddress.getPort());
    }

    public void start() throws IOException {
        register();
    }

    public void stop() throws IOException {}

    public void register() throws IOException {
        JsonObject ob = new JsonObject();
        ob.addProperty("id", this.raftServer.getId().toString());
        ob.addProperty("addr", this.listenAddress.toString());

        Gson gson = new GsonBuilder().create();
        String registerString = gson.toJson(ob);

        MediaType JSON = MediaType.parse("application/json; charset=utf-8");
        Request request = new Request.Builder()
                .url("http://"+this.interceptorAddress.toString()+"/replica")
                .post(RequestBody.create(JSON, registerString))
                .build();

        Response response = client.newCall(request).execute();
        if (response != null) {
            response.body().close();
        }
    }

    // TODO: SendEvent

    public String getNewRequestId() {
        return this.raftServer.getId().toString() + "_" + Integer.toString(this.counter.getAndIncrement());
    }

    public String getNewMessageId() {
        return this.random.ints(48, 123)
            .filter(i -> (i <= 57 || i >= 65) && (i <= 90 || i>= 97))
            .limit(16)
            .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
            .toString();
    }

    private void sendMessageToServer(String message) throws IOException {
        MediaType JSON = MediaType.parse("application/json; charset=utf-8");
        Request request = new Request.Builder()
                .url("http://"+this.interceptorAddress.toString()+"/message")
                .post(RequestBody.create(JSON, message))
                .build();

        Response response = client.newCall(request).execute();
        if (response != null) {
            response.body().close();
        }
    }

    public InterceptorMessage sendMessage(InterceptorMessage.Builder messageBuilder) throws IOException{
        // TODO:
        //  [X] need to construct a future to wait for a message on
        //  [x] use the message builder to construct a message after assigning message id, from address
        InterceptorMessage message = messageBuilder
            .setID(getNewMessageId())
            .setFrom(raftServer.getId().toString())
            .build();

        String requestId = message.getRequestId();
        CompletableFuture<InterceptorMessage> reply = new CompletableFuture<>();
        this.pollingThread.addPendingRequests(requestId, reply);

        sendMessageToServer(message.toJsonString());

        try {
            return reply.get(this.replyWaitTime.getDuration(), this.replyWaitTime.getUnit());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw IOUtils.toInterruptedIOException("Message sending interrupted", e);
        } catch (ExecutionException e) {
            throw IOUtils.toIOException(e);
        } catch (TimeoutException e) {
            throw new TimeoutIOException(e.getMessage(), e);
        }
    }

    // TODO: 
    //  [ ] polling functions that reads the messages
    //  [ ] completes the futures that were waiting
    //  [ ] calls the handler for the messages that are not pending ?
    private class MessagePollingThread extends Thread {
        private Map<String, CompletableFuture<InterceptorMessage>> pendingRequests;
        private InterceptorServer listenServer;
        private MessageHandler messageHandler;

        public MessagePollingThread(InterceptorServer listenServer, MessageHandler messageHandler) {
            this.listenServer = listenServer;
            this.pendingRequests = new ConcurrentHashMap<>();
            this.messageHandler = messageHandler;
        }

        public void addPendingRequests(String requestId, CompletableFuture<InterceptorMessage> reply) {
            this.pendingRequests.put(requestId, reply);
        }
    }

}
