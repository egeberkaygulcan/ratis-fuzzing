package org.apache.ratis.examples.common;

import java.util.Vector;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.server.GrpcLogAppender;
import org.apache.ratis.grpc.server.GrpcServerProtocolService;
import org.apache.ratis.proto.RaftProtos.RaftPeerRole;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.fuzzer.FuzzerClient;
import org.apache.ratis.server.impl.FollowerState;
import org.apache.ratis.server.impl.LeaderElection;
import org.apache.ratis.server.impl.MiniRaftCluster;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.statemachine.impl.SimpleStateMachine4Testing;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.ProtoUtils;
import org.apache.ratis.util.SizeInBytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;
import org.apache.ratis.util.Slf4jUtils;
import org.apache.ratis.util.TimeDuration;
import org.apache.ratis.statemachine.StateMachine;

public abstract class Experiments<CLUSTER extends MiniRaftCluster>
    implements MiniRaftCluster.Factory.Get<CLUSTER> {
        public static final Logger LOG = LoggerFactory.getLogger(Experiments.class);
        {
            Slf4jUtils.setLogLevel(RaftLog.LOG, Level.INFO);
            Slf4jUtils.setLogLevel(GrpcLogAppender.LOG, Level.DEBUG);
            Slf4jUtils.setLogLevel(LeaderElection.LOG, Level.DEBUG);
            Slf4jUtils.setLogLevel(GrpcServerProtocolService.LOG, Level.DEBUG);
            Slf4jUtils.setLogLevel(FollowerState.LOG, Level.DEBUG);
        }

        public int NUM_SERVERS;

        {
            final RaftProperties prop = getProperties();
            prop.setClass(MiniRaftCluster.STATEMACHINE_CLASS_KEY,
                    SimpleStateMachine4Testing.class, StateMachine.class);
            RaftServerConfigKeys.Log.setSegmentSizeMax(prop, SizeInBytes.valueOf("8KB"));
            RaftServerConfigKeys.Rpc.setTimeoutMin(prop, TimeDuration.valueOf(500, TimeUnit.MILLISECONDS));
            RaftServerConfigKeys.Rpc.setTimeoutMax(prop, TimeDuration.valueOf(1000, TimeUnit.MILLISECONDS));

        }

        public void startExperiment() throws Exception {
            System.out.println("Running Experiments");
            runWithNewCluster(NUM_SERVERS, this::runExperiment);
        }

        void runExperiment(MiniRaftCluster cluster) throws Exception {
            FuzzerClient fuzzerClient = FuzzerClient.getInstance("Experiments.java");
            int clientRequests = 0;
            Vector<String> restart;
            Vector<String> shutdown;
            boolean metadataRequest;
            boolean exit = false;
            final AtomicInteger messageCount = new AtomicInteger(0);
            final Supplier<Message> newMessage = () -> new SimpleMessage("m" + messageCount.getAndIncrement());


            // MiniRaftCluster.waitForLeader(cluster);
            // final RaftPeerId leaderId = cluster.getLeader().getId();
            // LOG.info("****** Raft LeaderId: {} ******", leaderId.toString());

            for(RaftServer s : cluster.getServers()) 
              fuzzerClient.registerServer(s.getId().toString());
          
            for (;;) {
                shutdown = fuzzerClient.getShutdown();
                restart = fuzzerClient.getRestart();
                clientRequests += fuzzerClient.getClientRequests();
                metadataRequest = fuzzerClient.metadataRequest();
                exit = exit || fuzzerClient.exitProcess();

                if (shutdown.size() > 0) {
                    for (String id : shutdown) {
                        RaftPeerId node = RaftPeerId.getRaftPeerId(id);
                        cluster.killServer(node);
                        cluster.servers.remove(node);
                    }
                    shutdown.clear();
                }

                if(restart.size() > 0) {
                    for (String id : restart) {
                        RaftPeerId node = RaftPeerId.getRaftPeerId(id);
                        final RaftServer proxy = cluster.putNewServer(node, cluster.getGroup(), false);
                        proxy.start();
                    }
                    restart.clear();
                }

                if (clientRequests > 0) 
                    if (cluster.getLeader() != null) 
                      for (; clientRequests > 0; clientRequests--)
                        writeSomething(newMessage, cluster);
  

                if (exit) {
                  System.out.println("Shutting down cluster");
                  cluster.shutdown();
                  break;
                }

                fuzzerClient.getAndExecuteMessages();
                Thread.sleep(1);
            }
        }

        static boolean writeSomething(Supplier<Message> newMessage, MiniRaftCluster cluster) throws Exception {
            try(final RaftClient client = cluster.createClient(cluster.getLeader().getPeer().getId())) {
                boolean succ = client.io().send(newMessage.get()).isSuccess();
                return succ == true;
            }
        }
    
}

class SimpleMessage implements Message {
    public static SimpleMessage[] create(int numMessages) {
      return create(numMessages, "m");
    }

    public static SimpleMessage[] create(int numMessages, String prefix) {
      final SimpleMessage[] messages = new SimpleMessage[numMessages];
      for (int i = 0; i < messages.length; i++) {
        messages[i] = new SimpleMessage(prefix + i);
      }
      return messages;
    }

    final String messageId;
    final ByteString bytes;

    public SimpleMessage(final String messageId) {
      this(messageId, ProtoUtils.toByteString(messageId));
    }

    public SimpleMessage(final String messageId, ByteString bytes) {
      this.messageId = messageId;
      this.bytes = bytes;
    }

    @Override
    public String toString() {
      return messageId;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == this) {
        return true;
      } else if (obj == null || !(obj instanceof SimpleMessage)) {
        return false;
      } else {
        final SimpleMessage that = (SimpleMessage)obj;
        return this.messageId.equals(that.messageId);
      }
    }

    @Override
    public int hashCode() {
      return messageId.hashCode();
    }

    @Override
    public ByteString getContent() {
      return bytes;
    }
  }
