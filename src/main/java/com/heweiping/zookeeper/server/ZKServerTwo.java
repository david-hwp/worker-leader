package com.heweiping.zookeeper.server;

import com.heweiping.zookeeper.util.ZookeeperServers;

/**
 * Created by WeiPing He on 2017/5/31 下午2:17.
 * Email: weiping_he@hansight.com
 */
public class ZKServerTwo {
    public static void main(String[] args) {
        new ZookeeperServers.ZooKeeperServerDistributed("2").start();
    }
}
