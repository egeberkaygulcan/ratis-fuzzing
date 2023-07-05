package org.apache.ratis.server.impl.comm;

import org.apache.ratis.server.impl.NetrixClient;

// TODO - Rename for Python fuzzer
public class NetrixClientSingleton {
    private static NetrixClient netrixClient;

    public static NetrixClient getClient(NetrixClientConfig c) {
        if (netrixClient == null)
            netrixClient = new NetrixClient(c);
        return netrixClient;
    } 

    public static NetrixClient getClient() {
        return netrixClient;
    }
}
