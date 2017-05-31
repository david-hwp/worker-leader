package com.heweiping.zookeeper.util;

import org.apache.log4j.BasicConfigurator;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.server.quorum.QuorumPeerMain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by WeiPing He on 2017/5/31 上午12:06.
 * Email: weiping_he@hansight.com
 */
public class ZookeeperServers {
    public static class ZooKeeperServerDistributed {
        private static Logger LOGGER = LoggerFactory.getLogger(ZooKeeperServerDistributed.class);
        private String serverId;
        private QuorumPeerMain quorumPeerMain;
        private QuorumPeerConfig qpc;

        /*******************************************************************************
         * RUN SERVER
         ******************************************************************************/
        public ZooKeeperServerDistributed(String serverId) {
            this.serverId = serverId;
            BasicConfigurator.configure();
            quorumPeerMain = new QuorumPeerMain();
            qpc = new QuorumPeerConfig();

            try {
                qpc.parse("src/main/resources/conf/zoo" + serverId + ".cfg");
            } catch (QuorumPeerConfig.ConfigException e) {
                e.printStackTrace();
            }
        }

        public void start() {
            LOGGER.info("SERVER {} :: STARTED", serverId);

            try {
                quorumPeerMain.runFromConfig(qpc);
            } catch (IOException e) {
                e.printStackTrace();
            }
            LOGGER.info("SERVER {} :: AFTER runFromConfig. Something is very wrong.", serverId);
        }
    }

    public static class ZooKeeperServerStandalone {
        private static Logger LOGGER = LoggerFactory.getLogger(ZooKeeperServerStandalone.class);

        /*******************************************************************************
         * RUN SERVER STANDALONE
         ******************************************************************************/
        public ZooKeeperServerStandalone() {
            BasicConfigurator.configure();
            ZooKeeperServerMain zkServer = new ZooKeeperServerMain();
            ServerConfig sc = new ServerConfig();
            QuorumPeerConfig qpc = new QuorumPeerConfig();

            try {
                qpc.parse("src/main/java/com/heweiping/zookeeper/conf/zoo1.cfg");
            } catch (QuorumPeerConfig.ConfigException e) {
                e.printStackTrace();
            }

            sc.readFrom(qpc);

            LOGGER.info("SERVER STANDALONE :: STARTED\n");

            try {
                zkServer.runFromConfig(sc);
            } catch (IOException e) {
                e.printStackTrace();
            }

            LOGGER.info("SERVER STANDALONE :: AFTER runFromConfig. Something is very wrong.\n");

        }

    }
}
