package org.apache.ratis.examples.common;

import org.apache.ratis.server.impl.MiniRaftCluster;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.examples.counter.CounterCommand;
import org.apache.ratis.examples.counter.server.CounterStateMachine;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.fuzzer.FuzzerClient;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.util.Slf4jUtils;
import org.apache.ratis.util.SizeInBytes;
import org.apache.ratis.util.TimeDuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public abstract class ExperimentCluster<CLUSTER extends MiniRaftCluster>
    implements MiniRaftCluster.Factory.Get<CLUSTER> {
  {
    Slf4jUtils.setLogLevel(RaftServer.Division.LOG, Level.INFO);
    Slf4jUtils.setLogLevel(RaftLog.LOG, Level.INFO);
    Slf4jUtils.setLogLevel(RaftClient.LOG, Level.INFO);
  }

  // public static int NUM_SERVERS;
  static final int NUM_SERVERS = 3;
  public final Logger LOG = LoggerFactory.getLogger(getClass());

  public static final TimeDuration HUNDRED_MILLIS = TimeDuration.valueOf(100, TimeUnit.MILLISECONDS);
  public static final TimeDuration ONE_SECOND = TimeDuration.ONE_SECOND;
  public static final TimeDuration FIVE_SECONDS = TimeDuration.valueOf(5, TimeUnit.SECONDS);

  {
    final RaftProperties prop = getProperties();
    prop.setClass(MiniRaftCluster.STATEMACHINE_CLASS_KEY,
        CounterStateMachine.class, StateMachine.class);
    RaftServerConfigKeys.Log.setSegmentSizeMax(prop, SizeInBytes.valueOf("8KB"));
    RaftServerConfigKeys.Snapshot.setAutoTriggerEnabled(prop, true);
    RaftServerConfigKeys.Read.setOption(prop, RaftServerConfigKeys.Read.Option.LINEARIZABLE);
    RaftServerConfigKeys.Read.setTimeout(prop, TimeDuration.ONE_SECOND);
  }

  public void controlledExperiment() throws Exception {
    runWithNewCluster(NUM_SERVERS, this::runControlledExperiment);
  }

  void runControlledExperiment(MiniRaftCluster cluster) throws Exception {
    // raftServer.getLifeCycleState() != LifeCycle.State.CLOSED
    FuzzerClient fuzzerClient = FuzzerClient.getInstance();
    fuzzerClient.registerCluster("1");

    ArrayList<String> crashList;
    ArrayList<String> restartList;

    final ArrayList<Future<RaftClientReply>> futures = new ArrayList<>();
    int clientRequests = 0;
    
    while(!fuzzerClient.shouldShutdown()) {
      // MiniRaftCluster.waitForLeader(cluster);
      // final RaftPeerId leaderId = cluster.getLeader().getId();
      /* ---------- CRASH SERVER ---------- */ 
      crashList = fuzzerClient.getCrash();
      if (crashList.size() > 0) {
        for (String id: crashList) {
          RaftPeerId peerId = RaftPeerId.getRaftPeerId(id);
          cluster.crashServer(peerId);
        }
      }


      /* ---------- RESTART SERVER ---------- */ 
      restartList = fuzzerClient.getRestart();
      if (restartList.size() > 0) {
        for (String id: restartList) {
          RaftPeerId peerId = RaftPeerId.getRaftPeerId(id);
          cluster.restartAfterCrash(peerId, false);
        }
      }
      

      /* ---------- SEND CLIENT REQUESTS ---------- */ 
      clientRequests = fuzzerClient.getClientRequests();
      if (clientRequests > 0) {
        ExecutorService executor = Executors.newFixedThreadPool(clientRequests);
        for(int i = 0; i < clientRequests; i++) {
          final Future<RaftClientReply> f = executor.submit(
            () -> {
              final RaftClient client = cluster.createClient();
              return client.io().send(CounterCommand.INCREMENT.getMessage());
            });
        futures.add(f);
        }
      }

      
      /* ---------- RECEIVE CLIENT REQUESTS ---------- */ 
      for (int i = 0; i < futures.size(); i++) {
        Future<RaftClientReply> f = futures.get(i);
        if (f.isDone()) {
          futures.remove(i);
          final RaftClientReply reply = f.get();
          if (reply.isSuccess()) {
            long timestamp = System.currentTimeMillis();
            final String count = reply.getMessage().getContent().toStringUtf8();
            System.out.println("Counter is incremented to " + count);
          } else {
            System.err.println("Failed " + reply);
          }
        } else if (f.isCancelled()) {
          futures.remove(i);
        }
      }
      // TODO - Linearizability check

      fuzzerClient.getAndExecuteMessages();
      TimeUnit.NANOSECONDS.sleep(100);
    }
    cluster.shutdown();
    return;
  }

//   public void testRestartFollower() throws Exception {
//     runWithNewCluster(NUM_SERVERS, this::runTestRestartFollower);
//   }

//   void runTestRestartFollower(MiniRaftCluster cluster) throws Exception {
//     MiniRaftCluster.waitForLeader(cluster);
//     final RaftPeerId leaderId = cluster.getLeader().getId();

//     // write some messages
//     final AtomicInteger messageCount = new AtomicInteger();
//     final Supplier<Message> newMessage = () -> new SimpleMessage("m" + messageCount.getAndIncrement());
//     writeSomething(newMessage, cluster);

//     // restart a follower
//     RaftPeerId followerId = cluster.getFollowers().get(0).getId();
//     LOG.info("Restart follower {}", followerId);
//     cluster.restartServer(followerId, false);

//     // write some more messages
//     writeSomething(newMessage, cluster);
//     final int truncatedMessageIndex = messageCount.get() - 1;

//     final long leaderLastIndex = cluster.getLeader().getRaftLog().getLastEntryTermIndex().getIndex();
//     // make sure the restarted follower can catchup
//     final RaftServer.Division followerState = cluster.getDivision(followerId);
//     JavaUtils.attemptRepeatedly(() -> {
//       assert followerState.getInfo().getLastAppliedIndex() >= leaderLastIndex;
//       return null;
//     }, 10, ONE_SECOND, "follower catchup", LOG);

//     // make sure the restarted peer's log segments is correct
//     final RaftServer.Division follower = cluster.restartServer(followerId, false);
//     final RaftLog followerLog = follower.getRaftLog();
//     final long followerLastIndex = followerLog.getLastEntryTermIndex().getIndex();
//     assert followerLastIndex >= leaderLastIndex;
//     final long leaderFinalIndex = cluster.getLeader().getRaftLog().getLastEntryTermIndex().getIndex();
//     assert leaderFinalIndex == followerLastIndex;

//     final File followerOpenLogFile = getOpenLogFile(follower);
//     final File leaderOpenLogFile = getOpenLogFile(cluster.getDivision(leaderId));

//     // shutdown all servers
//     // shutdown followers first, so there won't be any new leader elected
//     for (RaftServer.Division d : cluster.getFollowers()) {
//       d.close();
//     }
//     cluster.getDivision(leaderId).close();

//     // truncate log and
//     assertTruncatedLog(followerId, followerOpenLogFile, followerLastIndex, cluster);
//     assertTruncatedLog(leaderId, leaderOpenLogFile, leaderFinalIndex, cluster);

//     // restart and write something.
//     cluster.restart(false);
//     writeSomething(newMessage, cluster);

//     // restart again and check messages.
//     cluster.restart(false);
//     try(final RaftClient client = cluster.createClient()) {
//       for(int i = 0; i < messageCount.get(); i++) {
//         if (i != truncatedMessageIndex) {
//           final Message m = new SimpleMessage("m" + i);
//           final RaftClientReply reply = client.io().sendReadOnly(m);
//           assert reply.isSuccess();
//           LOG.info("query {}: {} {}", m, reply, LogEntryProto.parseFrom(reply.getMessage().getContent()));
//         }
//       }
//     }
//   }

//   static void writeSomething(Supplier<Message> newMessage, MiniRaftCluster cluster) throws Exception {
//     try(final RaftClient client = cluster.createClient()) {
//       // write some messages
//       for(int i = 0; i < 10; i++) {
//         assert client.io().send(newMessage.get()).isSuccess();
//       }
//     }
//   }

//   static void assertTruncatedLog(RaftPeerId id, File openLogFile, long lastIndex, MiniRaftCluster cluster) throws Exception {
//     // truncate log
//     if (openLogFile.length() > 0) {
//       FileUtils.truncateFile(openLogFile, openLogFile.length() - 1);
//     }
//     final RaftServer.Division server = cluster.restartServer(id, false);
//     // the last index should be one less than before
//     assert lastIndex - 1 == server.getRaftLog().getLastEntryTermIndex().getIndex();
//     server.getRaftServer().close();
//   }

//   static List<Path> getOpenLogFiles(RaftServer.Division server) throws Exception {
//     return LogSegmentPath.getLogSegmentPaths(server.getRaftStorage()).stream()
//         .filter(p -> p.getStartEnd().isOpen())
//         .map(LogSegmentPath::getPath)
//         .collect(Collectors.toList());
//   }

//   static File getOpenLogFile(RaftServer.Division server) throws Exception {
//     final List<Path> openLogs = getOpenLogFiles(server);
//     assert 1 == openLogs.size();
//     return openLogs.get(0).toFile();
//   }

//   public void testRestartWithCorruptedLogHeader() throws Exception {
//     runWithNewCluster(NUM_SERVERS, this::runTestRestartWithCorruptedLogHeader);
//   }

//   void runTestRestartWithCorruptedLogHeader(MiniRaftCluster cluster) throws Exception {
//     MiniRaftCluster.waitForLeader(cluster);
//     for(RaftServer.Division impl : cluster.iterateDivisions()) {
//       JavaUtils.attemptRepeatedly(() -> getOpenLogFile(impl), 10, TimeDuration.valueOf(100, TimeUnit.MILLISECONDS),
//           impl.getId() + ": wait for log file creation", LOG);
//     }

//     // shutdown all servers
//     for(RaftServer s : cluster.getServers()) {
//       s.close();
//     }

//     for(RaftServer.Division impl : cluster.iterateDivisions()) {
//       final File openLogFile = JavaUtils.attemptRepeatedly(() -> getOpenLogFile(impl),
//           10, HUNDRED_MILLIS, impl.getId() + "-getOpenLogFile", LOG);
//       for(int i = 0; i < SegmentedRaftLogFormat.getHeaderLength(); i++) {
//         assertCorruptedLogHeader(impl.getId(), openLogFile, i, cluster, LOG);
//         Assert.assertTrue(getOpenLogFiles(impl).isEmpty());
//       }
//     }
//   }

//   static void assertCorruptedLogHeader(RaftPeerId id, File openLogFile, int partialLength,
//       MiniRaftCluster cluster, Logger LOG) throws Exception {
//     Preconditions.assertTrue(partialLength < SegmentedRaftLogFormat.getHeaderLength());
//     try(final RandomAccessFile raf = new RandomAccessFile(openLogFile, "rw")) {
//       SegmentedRaftLogFormat.applyHeaderTo(header -> {
//         LOG.info("header    = {}", StringUtils.bytes2HexString(header));
//         final byte[] corrupted = new byte[header.length];
//         System.arraycopy(header, 0, corrupted, 0, partialLength);
//         LOG.info("corrupted = {}", StringUtils.bytes2HexString(corrupted));
//         raf.write(corrupted);
//         return null;
//       });
//     }
//     final RaftServer.Division server = cluster.restartServer(id, false);
//     server.getRaftServer().close();
//   }

//   public Iterable<LogEntryProto> getLogEntryProtos(RaftLog log) {
//     return CollectionUtils.as(log.getEntries(0, Long.MAX_VALUE), ti -> {
//       try {
//         return log.get(ti.getIndex());
//       } catch (IOException exception) {
//         throw new AssertionError("Failed to get log at " + ti, exception);
//       }
//     });
//   }

//   public List<LogEntryProto> getStateMachineLogEntries(RaftLog log) {
//     final List<LogEntryProto> entries = new ArrayList<>();
//     for (LogEntryProto e : getLogEntryProtos(log)) {
//       final String s = LogProtoUtils.toLogEntryString(e);
//       if (e.hasStateMachineLogEntry()) {
//         LOG.info(s + ", " + e.getStateMachineLogEntry().toString().trim().replace("\n", ", "));
//         entries.add(e);
//       } else if (e.hasConfigurationEntry()) {
//         LOG.info("Found {}, ignoring it.", s);
//       } else if (e.hasMetadataEntry()) {
//         LOG.info("Found {}, ignoring it.", s);
//       } else {
//         throw new AssertionError("Unexpected LogEntryBodyCase " + e.getLogEntryBodyCase() + " at " + s);
//       }
//     }
//     return entries;
//   }

//   static void assertSameLog(RaftLog expected, RaftLog computed) throws Exception {
//     Assert.assertEquals(expected.getLastEntryTermIndex(), computed.getLastEntryTermIndex());
//     final long lastIndex = expected.getNextIndex() - 1;
//     Assert.assertEquals(expected.getLastEntryTermIndex().getIndex(), lastIndex);
//     for(long i = 0; i < lastIndex; i++) {
//       Assert.assertEquals(expected.get(i), computed.get(i));
//     }
//   }

//   public void testRestartCommitIndex() throws Exception {
//     runWithNewCluster(NUM_SERVERS, this::runTestRestartCommitIndex);
//   }

//   void runTestRestartCommitIndex(MiniRaftCluster cluster) throws Exception {
//     final SimpleMessage[] messages = SimpleMessage.create(10);
//     final List<CompletableFuture<Void>> futures = new ArrayList<>(messages.length);
//     for(int i = 0; i < messages.length; i++) {
//       final CompletableFuture<Void> f = new CompletableFuture<>();
//       futures.add(f);

//       final SimpleMessage m = messages[i];
//       new Thread(() -> {
//         try (final RaftClient client = cluster.createClient()) {
//           Assert.assertTrue(client.io().send(m).isSuccess());
//         } catch (IOException e) {
//           throw new IllegalStateException("Failed to send " + m, e);
//         }
//         f.complete(null);
//       }).start();
//     }
//     JavaUtils.allOf(futures).get();
//     LOG.info("sent {} messages.", messages.length);

//     final List<RaftPeerId> ids = new ArrayList<>();
//     final RaftServer.Division leader = cluster.getLeader();
//     final RaftLog leaderLog = leader.getRaftLog();
//     final RaftPeerId leaderId = leader.getId();
//     ids.add(leaderId);

//     getStateMachineLogEntries(leaderLog);

//     // check that the last metadata entry is written to the log
//     JavaUtils.attempt(() -> assertLastLogEntry(leader), 20, HUNDRED_MILLIS, "leader last metadata entry", LOG);

//     final long lastIndex = leaderLog.getLastEntryTermIndex().getIndex();
//     LOG.info("{}: leader lastIndex={}", leaderId, lastIndex);
//     final LogEntryProto lastEntry = leaderLog.get(lastIndex);
//     LOG.info("{}: leader lastEntry entry[{}] = {}", leaderId, lastIndex, LogProtoUtils.toLogEntryString(lastEntry));
//     final long loggedCommitIndex = lastEntry.getMetadataEntry().getCommitIndex();
//     final LogEntryProto lastCommittedEntry = leaderLog.get(loggedCommitIndex);
//     LOG.info("{}: leader lastCommittedEntry = entry[{}] = {}",
//         leaderId, loggedCommitIndex, LogProtoUtils.toLogEntryString(lastCommittedEntry));

//     final ArithmeticStateMachine leaderStateMachine = ArithmeticStateMachine.get(leader);
//     final TermIndex lastAppliedTermIndex = leaderStateMachine.getLastAppliedTermIndex();
//     LOG.info("{}: leader lastAppliedTermIndex = {}", leaderId, lastAppliedTermIndex);

//     // check follower logs
//     for(RaftServer.Division s : cluster.iterateDivisions()) {
//       if (!s.getId().equals(leaderId)) {
//         ids.add(s.getId());
//         JavaUtils.attempt(() -> assertSameLog(leaderLog, s.getRaftLog()),
//             10, HUNDRED_MILLIS, "assertRaftLog-" + s.getId(), LOG);
//       }
//     }

//     // take snapshot and truncate last (metadata) entry
//     leaderStateMachine.takeSnapshot();
//     leaderLog.truncate(lastIndex);

//     // kill all servers
//     ids.forEach(cluster::killServer);

//     // Restart and kill servers one by one so that they won't talk to each other.
//     for(RaftPeerId id : ids) {
//       cluster.restartServer(id, false);
//       final RaftServer.Division server = cluster.getDivision(id);
//       final RaftLog raftLog = server.getRaftLog();
//       JavaUtils.attemptRepeatedly(() -> {
//         Assert.assertTrue(raftLog.getLastCommittedIndex() >= loggedCommitIndex);
//         return null;
//       }, 10, HUNDRED_MILLIS, id + "(commitIndex >= loggedCommitIndex)", LOG);
//       JavaUtils.attemptRepeatedly(() -> {
//         Assert.assertTrue(server.getInfo().getLastAppliedIndex() >= loggedCommitIndex);
//         return null;
//       }, 10, HUNDRED_MILLIS, id + "(lastAppliedIndex >= loggedCommitIndex)", LOG);
//       LOG.info("{}: commitIndex={}, lastAppliedIndex={}",
//           id, raftLog.getLastCommittedIndex(), server.getInfo().getLastAppliedIndex());
//       cluster.killServer(id);
//     }
//   }

//   static void assertLastLogEntry(RaftServer.Division server) throws RaftLogIOException {
//     final RaftLog raftLog = server.getRaftLog();
//     final long lastIndex = raftLog.getLastEntryTermIndex().getIndex();
//     final LogEntryProto lastEntry = raftLog.get(lastIndex);
//     Assert.assertTrue(lastEntry.hasMetadataEntry());

//     final long loggedCommitIndex = lastEntry.getMetadataEntry().getCommitIndex();
//     final LogEntryProto lastCommittedEntry = raftLog.get(loggedCommitIndex);
//     Assert.assertTrue(lastCommittedEntry.hasStateMachineLogEntry());

//     final ArithmeticStateMachine leaderStateMachine = ArithmeticStateMachine.get(server);
//     final TermIndex lastAppliedTermIndex = leaderStateMachine.getLastAppliedTermIndex();
//     Assert.assertEquals(lastCommittedEntry.getTerm(), lastAppliedTermIndex.getTerm());
//     Assert.assertTrue(lastCommittedEntry.getIndex() <= lastAppliedTermIndex.getIndex());
//   }
}


