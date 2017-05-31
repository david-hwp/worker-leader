package com.heweiping.zookeeper.worker;

import com.heweiping.zookeeper.util.Worker;

/**
 * Created by WeiPing He on 2017/5/30 下午11:23.
 * Email: weiping_he@hansight.com
 */
public class WorkerOne {
    public static void main(String[] args) throws Exception {
//        new MyWorkerThread().setServer(new ZookeeperServers.ZooKeeperServerDistributed("1")).start();
        new MyWorkerThread().setWorker(new Worker("1")).start();
    }
}
