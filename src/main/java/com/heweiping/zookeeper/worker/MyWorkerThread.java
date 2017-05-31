package com.heweiping.zookeeper.worker;

import com.heweiping.zookeeper.util.Worker;
import com.heweiping.zookeeper.util.ZookeeperServers;

/**
 * Created by WeiPing He on 2017/5/31 上午12:20.
 * Email: weiping_he@hansight.com
 */
public class MyWorkerThread extends Thread {

    private Worker worker = null;
    private ZookeeperServers.ZooKeeperServerDistributed zooKeeperServerDistributed = null;

    @Override
    public void run() {
        try {
            if (zooKeeperServerDistributed != null) {
                zooKeeperServerDistributed.start();
            }
            if (worker != null) {
                worker.start();

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public MyWorkerThread setWorker(Worker worker) {
        this.worker = worker;
        return this;
    }

    public MyWorkerThread setServer(ZookeeperServers.ZooKeeperServerDistributed zooKeeperServerDistributed) {
        this.zooKeeperServerDistributed = zooKeeperServerDistributed;
        return this;
    }
}