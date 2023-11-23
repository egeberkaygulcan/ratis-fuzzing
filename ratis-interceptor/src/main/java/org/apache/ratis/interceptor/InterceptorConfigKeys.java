package org.apache.ratis.interceptor;

import org.apache.ratis.conf.RaftProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;

import static org.apache.ratis.conf.ConfUtils.*;
import static org.apache.ratis.conf.ConfUtils.setInt;

public interface InterceptorConfigKeys {

    String PREFIX="raft.interceptor";

    interface Server {
        Logger LOG = LoggerFactory.getLogger(Server.class);

        static Consumer<String> getDefaultLog() {
            return LOG::info;
        }

        String PREFIX = InterceptorConfigKeys.PREFIX + ".server";
        String HOST_KEY = PREFIX + ".host";
        String HOST_DEFAULT = "127.0.0.1";

        String PORT_KEY = PREFIX + ".port";
        int PORT_DEFAULT = 7074;

        static String host(RaftProperties properties) {
            return get(properties::get, HOST_KEY, HOST_DEFAULT, getDefaultLog());
        }

        static void setHost(RaftProperties properties, String host) {
            set(properties::set, HOST_KEY, host);
        }

        static int port(RaftProperties properties) {
            return getInt(properties::getInt,
                    PORT_KEY, PORT_DEFAULT, getDefaultLog(), requireMin(0), requireMax(65536));
        }

        static void setPort(RaftProperties properties, int port) {
            setInt(properties::setInt, PORT_KEY, port);
        }
    }

    interface InterceptorListener {
        Logger LOG = LoggerFactory.getLogger(InterceptorListener.class);

        static Consumer<String> getDefaultLog() {
            return LOG::info;
        }

        String PREFIX = InterceptorConfigKeys.PREFIX + ".ilistener";
        String HOST_KEY = PREFIX + ".host";
        String HOST_DEFAULT = "127.0.0.1";

        String PORT_KEY = PREFIX + ".port";
        int PORT_DEFAULT = 2023;

        static String host(RaftProperties properties) {
            return get(properties::get, HOST_KEY, HOST_DEFAULT, getDefaultLog());
        }

        static void setHost(RaftProperties properties, String host) {
            set(properties::set, HOST_KEY, host);
        }

        static int port(RaftProperties properties) {
            return getInt(properties::getInt,
                    PORT_KEY, PORT_DEFAULT, getDefaultLog(), requireMin(0), requireMax(65536));
        }

        static void setPort(RaftProperties properties, int port) {
            setInt(properties::setInt, PORT_KEY, port);
        }
    }

    interface Listener {
        Logger LOG = LoggerFactory.getLogger(Listener.class);

        static Consumer<String> getDefaultLog() {
            return LOG::info;
        }

        String PREFIX = InterceptorConfigKeys.PREFIX + ".listener";
        String HOST_KEY = PREFIX + ".host";
        String HOST_DEFAULT = "127.0.0.1";

        String PORT_KEY = PREFIX + ".port";
        int PORT_DEFAULT = 3023;

        static String host(RaftProperties properties) {
            return get(properties::get, HOST_KEY, HOST_DEFAULT, getDefaultLog());
        }

        static void setHost(RaftProperties properties, String host) {
            set(properties::set, HOST_KEY, host);
        }

        static int port(RaftProperties properties) {
            return getInt(properties::getInt,
                    PORT_KEY, PORT_DEFAULT, getDefaultLog(), requireMin(0), requireMax(65536));
        }

        static void setPort(RaftProperties properties, int port) {
            setInt(properties::setInt, PORT_KEY, port);
        }
    }

    static void main(String[] args) {
        printAll(InterceptorConfigKeys.class);
    }
}
