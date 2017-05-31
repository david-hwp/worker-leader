package com.heweiping.zookeeper.worker;

import com.heweiping.zookeeper.util.Worker;
import com.heweiping.zookeeper.util.ZookeeperServers;

/**
 * Created by WeiPing He on 2017/5/30 下午11:23.
 * Email: weiping_he@hansight.com
 */
public class WorkerThree {
    public static void main(String[] args) throws Exception {
//        new MyWorkerThread().setServer(new ZookeeperServers.ZooKeeperServerDistributed("3")).start();
        new MyWorkerThread().setWorker(new Worker("3")).start();
    }
}
