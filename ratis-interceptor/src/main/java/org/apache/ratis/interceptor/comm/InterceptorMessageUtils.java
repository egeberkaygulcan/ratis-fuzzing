package org.apache.ratis.interceptor.comm;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.ratis.proto.RaftProtos.*;
import org.apache.ratis.thirdparty.com.google.protobuf.util.JsonFormat;

public class InterceptorMessageUtils {
    public static byte[] fromRequestVoteRequest(RequestVoteRequestProto request) throws IOException{
        String out = JsonFormat.printer().print(request);
        return out.getBytes(StandardCharsets.UTF_8);
    }

    public static byte[] fromRequestVoteReply(RequestVoteReplyProto reply) throws IOException{
        String out = JsonFormat.printer().print(reply);
        return out.getBytes(StandardCharsets.UTF_8);
    }

    public static byte[] fromAppendEntriesRequest(AppendEntriesRequestProto request) throws IOException{
        String out = JsonFormat.printer().print(request);
        return out.getBytes(StandardCharsets.UTF_8);
    }

    public static byte[] fromAppendEntriesReply(AppendEntriesReplyProto reply) throws IOException {
        String out = JsonFormat.printer().print(reply);
        return out.getBytes(StandardCharsets.UTF_8);
    }

    public RequestVoteRequestProto toRequestVoteRequest(byte[] data) throws IOException{
        String jsonData = new String(data, StandardCharsets.UTF_8);

        RequestVoteRequestProto.Builder builder = RequestVoteRequestProto.newBuilder();
        JsonFormat.parser().ignoringUnknownFields().merge(jsonData, builder);

        return builder.build();
    }

    public RequestVoteReplyProto toRequestVoteReply(byte[] data) throws IOException{
        String jsonData = new String(data, StandardCharsets.UTF_8);

        RequestVoteReplyProto.Builder builder = RequestVoteReplyProto.newBuilder();
        JsonFormat.parser().ignoringUnknownFields().merge(jsonData, builder);

        return builder.build();
    }

    public AppendEntriesRequestProto toAppendEntriesRequest(byte[] data) throws IOException{
        String jsonData = new String(data, StandardCharsets.UTF_8);

        AppendEntriesRequestProto.Builder builder = AppendEntriesRequestProto.newBuilder();
        JsonFormat.parser().ignoringUnknownFields().merge(jsonData, builder);

        return builder.build();
    }

    public AppendEntriesReplyProto toAppendEntriesReply(byte[] data) throws IOException{
        String jsonData = new String(data, StandardCharsets.UTF_8);

        AppendEntriesReplyProto.Builder builder = AppendEntriesReplyProto.newBuilder();
        JsonFormat.parser().ignoringUnknownFields().merge(jsonData, builder);

        return builder.build();
    }
}
