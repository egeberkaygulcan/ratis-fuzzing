package org.apache.ratis.examples.common;

public class ClusterWrapper
    extends ExperimentCluster<MiniRaftClusterWithGrpc>
    implements MiniRaftClusterWithGrpc.FactoryGet {

    private final int numServers;

    public ClusterWrapper(int numNodes) {
        numServers = numNodes;
    }

    public void run() {
        // ExperimentCluster.NUM_SERVERS = numServers;
        try {
            this.controlledExperiment();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }
}
