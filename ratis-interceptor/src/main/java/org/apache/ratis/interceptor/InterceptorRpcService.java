package org.apache.ratis.interceptor;

import org.apache.ratis.interceptor.comm.InterceptorClient;
import org.apache.ratis.interceptor.comm.InterceptorMessage;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerRpcWithProxy;
import org.apache.ratis.proto.RaftProtos.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;

public class InterceptorRpcService extends RaftServerRpcWithProxy<InterceptorRpcProxy, InterceptorRpcProxy.PeerMap> {
    public static final Logger LOG = LoggerFactory.getLogger(InterceptorRpcService.class);

    private final RaftServer raftServer;
    private final InetSocketAddress listenerAddress;
    private final InetSocketAddress serverAddress;

    private final InterceptorClient iClient;

    public InterceptorRpcService(RaftServer server) {
        super(server::getId, id -> new InterceptorRpcProxy.PeerMap(id.toString(), server.getProperties()));
        this.raftServer = server;

        final String lhost = InterceptorConfigKeys.Listener.host(server.getProperties());
        final int lport = InterceptorConfigKeys.Listener.port(server.getProperties());

        this.listenerAddress = new InetSocketAddress(lhost, lport);

        final String shost = InterceptorConfigKeys.Server.host(server.getProperties());
        final int sport = InterceptorConfigKeys.Server.port(server.getProperties());

        this.serverAddress = new InetSocketAddress(lhost, lport);

        this.iClient = new InterceptorClient(server, this.serverAddress, this.listenerAddress);
    }

    @Override
    public SupportedRpcType getRpcType() { return SupportedRpcType.INTERCEPTOR; }

    @Override
    public void startImpl() throws IOException {
        this.iClient.start();
    }

    @Override
    public void closeImpl() throws IOException {
        this.iClient.stop();
    }

    @Override
    public InetSocketAddress getInetSocketAddress() {
        return listenerAddress;
    }

    @Override
    public RequestVoteReplyProto requestVote(RequestVoteRequestProto request) throws IOException {
        InterceptorMessage.Builder iMessageBuilder = new InterceptorMessage.Builder()
                .setRequestVoteRequest(request)
                .setRequestId(iClient.getNewRequestId());

        // todo: poll on a future and return once you have the reply
        iClient.sendMessage(iMessageBuilder);

        return null;
    }

    @Override
    public AppendEntriesReplyProto appendEntries(AppendEntriesRequestProto request) throws IOException {
        return null;
    }

    @Override
    public InstallSnapshotReplyProto installSnapshot(InstallSnapshotRequestProto request) throws IOException {
        return null;
    }

    @Override
    public StartLeaderElectionReplyProto startLeaderElection(StartLeaderElectionRequestProto request) throws IOException {
        return null;
    }
}
