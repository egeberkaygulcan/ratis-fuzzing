package org.apache.ratis.server.fuzzer.messages;

import org.apache.ratis.proto.RaftProtos.RequestVoteReplyProto;
import org.apache.ratis.proto.RaftProtos.RequestVoteRequestProto;
import org.apache.ratis.thirdparty.io.grpc.internal.GrpcUtil;
import org.apache.ratis.thirdparty.io.grpc.stub.StreamObserver;

public class RequestVoteReplyMessage extends Message {

    private RequestVoteReplyProto request;
    StreamObserver<RequestVoteReplyProto> responseObserver;

    public RequestVoteReplyMessage(RequestVoteReplyProto r, StreamObserver<RequestVoteReplyProto> ro) {
        this.request = r;
        this.responseObserver = ro;
        this.setType("request_vote_response");
        this.setId(this.client.generateId());

        isControlledExecution();
    }

    @Override
    public void invoke() {
        try {
            this.responseObserver.onNext(this.request);
            this.responseObserver.onCompleted();
        } catch (Exception e) {
            System.out.println("RequestVoteReply failed");
            e.printStackTrace();

        }
    }

    @Override
    public void send() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'send'");
    }

    @Override
    public String getReceiver() {
        return null;
    }

    @Override
    protected String toJsonString() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'toJsonString'");
    }
    
}
