package org.apache.ratis.server.fuzzer.comm;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.*;
import io.netty.util.CharsetUtil;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Vector;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.ratis.server.fuzzer.FuzzerClient;

import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpHeaderValues.APPLICATION_JSON;

public class MessageHandler{

    private Vector<JsonMessage> messages;
    private final ReentrantLock lock = new ReentrantLock();

    public MessageHandler() {
        this.messages = new Vector<JsonMessage>();
    }

    public FullHttpResponse handle(FullHttpRequest req) {
        try {
            FuzzerClient fuzzerClient = FuzzerClient.getInstance("Fuzzer MessageHandler.java");
            JsonMessage m = getMessageFromReq(req);
            lock.lock();
            messages.add(m);
            lock.unlock();

            if (m.type.equals("client_request")) {
                fuzzerClient.scheduleClientReq();
            } else if (m.type.equals("crash")) {
                fuzzerClient.scheduleShutdown(Integer.parseInt(new String(m.data, StandardCharsets.UTF_8))); // Base64.getEncoder().encodeToString(bytes);
            } else if (m.type.equals("restart")) {
                fuzzerClient.scheduleRestart(Integer.parseInt(new String(m.data, StandardCharsets.UTF_8))); // Base64.getEncoder().encodeToString(bytes);
            } else if (m.type.equals("exit")) {
                fuzzerClient.startExit();
            } else {
                lock.lock();
                messages.add(m);
                lock.unlock();
            }

            HashMap<String, String> params = new HashMap<String, String>();
            params.put("message_id", m.getId());

            return new DefaultFullHttpResponse(
                    req.protocolVersion(),
                    HttpResponseStatus.OK,
                    Unpooled.copiedBuffer("Ok", CharsetUtil.UTF_8)
            );
        } catch (Exception e) {
            e.printStackTrace();
            return new DefaultFullHttpResponse(
                    req.protocolVersion(),
                    HttpResponseStatus.INTERNAL_SERVER_ERROR
            );
        }
    }

    private JsonMessage getMessageFromReq(FullHttpRequest req) throws IOException {
        ByteBuf content = req.content();
        if(content == null || content.readableBytes() <= 0){
            throw new IOException("empty request");
        }
        if(!req.headers().get(CONTENT_TYPE).equals(APPLICATION_JSON.toString())) {
            throw new IOException("not a json request");
        }
        String str = content.toString(CharsetUtil.UTF_8);
        return JsonMessage.fromJsonString(str.substring(1, str.length() - 1).replace("\\", ""));
    }

    public Vector<JsonMessage> getMessages() {
        Vector<JsonMessage> result = new Vector<JsonMessage>();
        lock.lock();
        result.addAll(messages);
        messages.clear();
        lock.unlock();
        return result;
    }
}
