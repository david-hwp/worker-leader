package com.heweiping.zookeeper.worker;

import com.heweiping.zookeeper.util.Worker;
import com.heweiping.zookeeper.util.ZookeeperServers;

/**
 * Created by WeiPing He on 2017/5/30 下午11:23.
 * Email: weiping_he@hansight.com
 */
public class WorkerTwo {
    public static void main(String[] args) throws Exception {
//        new MyWorkerThread().setServer(new ZookeeperServers.ZooKeeperServerDistributed("2")).start();
        new MyWorkerThread().setWorker(new Worker("2")).start();
    }
}
