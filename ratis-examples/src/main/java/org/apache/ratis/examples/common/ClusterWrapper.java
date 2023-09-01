package org.apache.ratis.examples.common;

public class ClusterWrapper
    extends ExperimentCluster<MiniRaftClusterWithGrpc>
    implements MiniRaftClusterWithGrpc.FactoryGet {

    public void run(int numNodes) {
        ExperimentCluster.NUM_SERVERS = numNodes;
        try {
            this.controlledExperiment();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }
}
