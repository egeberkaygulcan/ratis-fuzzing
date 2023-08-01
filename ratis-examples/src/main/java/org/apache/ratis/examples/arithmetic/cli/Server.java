/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ratis.examples.arithmetic.cli;

import org.apache.ratis.server.impl.MiniRaftCluster;
import org.apache.ratis.examples.arithmetic.expression.DoubleValue;
import org.apache.ratis.examples.arithmetic.expression.Expression;
import org.apache.ratis.examples.arithmetic.expression.NullValue;
import org.apache.ratis.examples.arithmetic.expression.Variable;

import static org.apache.ratis.examples.arithmetic.expression.BinaryExpression.Op.*;
import static org.apache.ratis.examples.arithmetic.expression.UnaryExpression.Op.SQRT;
import static org.apache.ratis.examples.arithmetic.expression.UnaryExpression.Op.SQUARE;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.examples.arithmetic.ArithmeticStateMachine;
import org.apache.ratis.examples.common.SubCommandBase;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.grpc.MiniRaftClusterWithGrpc;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.LifeCycle;
import org.apache.ratis.util.NetUtils;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.TimeDuration;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Class to start a ratis arithmetic example server.
 */
@Parameters(commandDescription = "Start an arithmetic server")
public class Server extends SubCommandBase {
  public MiniRaftCluster cluster;
  private static final AtomicReference<MiniRaftCluster> currentCluster = new AtomicReference<>();

  @Parameter(names = {"--id",
      "-i"}, description = "Raft id of this server", required = false)
  private String id;

  @Parameter(names = {"--storage",
      "-s"}, description = "Storage dir", required = false)
  private File storageDir;

  @Override
  public void run() throws Exception {
    // RaftPeerId peerId = RaftPeerId.valueOf(id);
    // RaftProperties properties = new RaftProperties();

    // final int port = NetUtils.createSocketAddr(getPeer(peerId).getAddress()).getPort();
    // GrpcConfigKeys.Server.setPort(properties, port);

    // Optional.ofNullable(getPeer(peerId).getClientAddress()).ifPresent(address ->
    //     GrpcConfigKeys.Client.setPort(properties, NetUtils.createSocketAddr(address).getPort()));
    // Optional.ofNullable(getPeer(peerId).getAdminAddress()).ifPresent(address ->
    //     GrpcConfigKeys.Admin.setPort(properties, NetUtils.createSocketAddr(address).getPort()));

    // RaftServerConfigKeys.setStorageDir(properties, Collections.singletonList(storageDir));
    // StateMachine stateMachine = new ArithmeticStateMachine();

    // final RaftGroup raftGroup = RaftGroup.valueOf(RaftGroupId.valueOf(ByteString.copyFromUtf8(getRaftGroupId())),
    //         getPeers());
    // RaftServer raftServer = RaftServer.newBuilder()
    //     .setServerId(RaftPeerId.valueOf(id))
    //     .setStateMachine(stateMachine).setProperties(properties)
    //     .setGroup(raftGroup)
    //     .build();
    // raftServer.start();

    // for(; raftServer.getLifeCycleState() != LifeCycle.State.CLOSED;) {
    //   TimeUnit.SECONDS.sleep(1);
    // }

    testPythagorean(3);
  }

  public void testPythagorean(int clusterSize) throws Exception {
    RaftProperties prop = new RaftProperties();
    RaftPeerId peerId = RaftPeerId.valueOf(id);

    final int port = NetUtils.createSocketAddr(getPeer(peerId).getAddress()).getPort();
    GrpcConfigKeys.Server.setPort(prop, port);

    // avoid flaky behaviour in CI environment
    RaftServerConfigKeys.Rpc.setTimeoutMin(prop, TimeDuration.valueOf(300, TimeUnit.MILLISECONDS));
    RaftServerConfigKeys.Rpc.setTimeoutMax(prop, TimeDuration.valueOf(600, TimeUnit.MILLISECONDS));

    RaftServerConfigKeys.setStorageDir(prop, Collections.singletonList(storageDir));

    prop.setClass(MiniRaftCluster.STATEMACHINE_CLASS_KEY,
        ArithmeticStateMachine.class, StateMachine.class); 
        
    String[] ids = MiniRaftCluster.generateIds(clusterSize, 0);

    final MiniRaftCluster cluster = MiniRaftClusterWithGrpc.FACTORY.newCluster(ids, new String[] {}, prop);

    try (final RaftClient client = cluster.createClient()) {
      runTestPythagorean(client, 3, 10);
    }

    cluster.close();
  }

  public void runTestPythagorean(
      RaftClient client, int start, int count) throws IOException {
    Preconditions.assertTrue(count > 0, () -> "count = " + count + " <= 0");
    Preconditions.assertTrue(start >= 2, () -> "start = " + start + " < 2");

    final Variable a = new Variable("a");
    final Variable b = new Variable("b");
    final Variable c = new Variable("c");
    final Expression pythagorean = SQRT.apply(ADD.apply(SQUARE.apply(a), SQUARE.apply(b)));

    final int end = start + 2*count;
    for(int n = (start & 1) == 0? start + 1: start; n < end; n += 2) {
      int n2 = n*n;
      int half_n2 = n2/2;

      try {
        assign(client, a, n);
        assign(client, b, half_n2);
        assign(client, c, pythagorean, (double)half_n2 + 1);

        assignNull(client, a);
        assignNull(client, b);
        assignNull(client, c);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  private Expression assign(RaftClient client, Variable x, double value) throws IOException, Exception {
    return assign(client, x, new DoubleValue(value), value);
  }

  private Expression assign(RaftClient client, Variable x, Expression e, Double expected) throws IOException, Exception {
    final RaftClientReply r = client.io().send(x.assign(e));
    return assertRaftClientReply(r, expected);
  }

  private Expression assign(RaftClient client, Variable x, Expression e) throws IOException, Exception {
    return assign(client, x, e, null);
  }

  private Expression assertRaftClientReply(RaftClientReply reply, Double expected) throws Exception{
    final Expression e;
    if(reply.isSuccess()) {
      e = Expression.Utils.bytes2Expression(
          reply.getMessage().getContent().toByteArray(), 0);
      if (expected != null) {
        if(expected != e.evaluate(null))
          throw new Exception("****** Client reply not equal to expected! ******");
      }
    } else {
      throw new Exception("****** Client reply unsuccesfull! ******");
    }
    return e;
  }

  private void assignNull(RaftClient client, Variable x) throws IOException, Exception {
    final Expression e = assign(client, x, NullValue.getInstance());
    if(NullValue.getInstance() != e)
      throw new Exception("****** assignNull failed! ******");
  }

}
