package org.apache.ratis.examples.common;

import org.apache.ratis.grpc.MiniRaftClusterWithGrpc;

public class ExperimentRunner 
    extends Experiments<MiniRaftClusterWithGrpc>
    implements MiniRaftClusterWithGrpc.FactoryGet{

    public ExperimentRunner(int clusterSize) {
        this.NUM_SERVERS = clusterSize;
    }
    
}
