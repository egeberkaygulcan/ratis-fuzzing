package org.apache.ratis.server.impl.comm;

import java.util.HashMap;

public class NetrixClientConfig {

    public String replicaID;

    public String netrixAddr;
    public String clientServerAddr;
    public int clientServerPort;
    public String clientAdvAddr;

    public HashMap<String, String> info;

    public NetrixClientConfig(String id, String nAddr, String cSAddr, int cSPort, String cAAddr, HashMap<String, String> info) {
        this.replicaID = id;
        this.netrixAddr = nAddr;
        this.clientAdvAddr = cAAddr;
        this.clientServerAddr = cSAddr;
        this.clientServerPort = cSPort;
        this.info = info;
    }

    public NetrixClientConfig(String id, String nAddr, String cSAddr, HashMap<String, String> info) {
        this.replicaID = id;
        this.netrixAddr = nAddr;
        this.clientServerAddr = cSAddr;
        this.clientAdvAddr = cSAddr;
        this.info = info;
    }
}
