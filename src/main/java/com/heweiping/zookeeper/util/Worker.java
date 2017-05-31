package com.heweiping.zookeeper.util;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.recipes.nodes.PersistentEphemeralNode;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.heweiping.zookeeper.util.CuratorUtils.*;
import static org.apache.curator.framework.recipes.nodes.PersistentEphemeralNode.Mode.EPHEMERAL;

public class Worker {

    private static final Logger LOGGER = LoggerFactory.getLogger(Worker.class);

    private String ID = "";

    private CuratorFramework client = getCuratorFramework();

    public Worker(String id) {
        this.ID = id;
    }

    public void start() throws Exception {
        //创建Worker运行时目录
        Stat stat = client.checkExists().forPath(RUNNING_ROOT + WORKERS_PATH);
        if (stat == null) {
            client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(RUNNING_ROOT + WORKERS_PATH);
        }
        //创建Worker运行时目录（Session断开表示该Worker失联，对应目录会自动消失）
        PersistentEphemeralNode node = new PersistentEphemeralNode(client, EPHEMERAL, RUNNING_ROOT + WORKERS_PATH + "/worker_" + ID, new byte[0]);
        node.start();
        node.waitForInitialCreate(3, TimeUnit.SECONDS);
        String actualPath = node.getActualPath();
        LOGGER.info("Worker_" + ID + ":node " + actualPath + " value: " + new String(client.getData().forPath(actualPath)));

        setDataSourceListener(client, STORE_ROOT + DATASOURCES_PATH);
        Thread.sleep(Integer.MAX_VALUE);
    }

    private void setDataSourceListener(CuratorFramework client, String path) throws Exception {
        PathChildrenCache childrenCache = new PathChildrenCache(client, path, true);
        PathChildrenCacheListener childrenCacheListener = new PathChildrenCacheListener() {
            @Override
            public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
                ChildData data = event.getData();
                List<String> tmp = Arrays.asList(data.getPath().split("/"));
                String datasource = tmp.get(tmp.size() - 1);
                List<String> workers = Arrays.asList(new String(data.getData()).split(","));
                switch (event.getType()) {
                    case CHILD_ADDED:
                        LOGGER.info("Worker_" + ID + ":CHILD_ADDED : " + data.getPath() + "  数据:" + new String(data.getData()));
                        workerElection(Worker.this.client, datasource, workers);
                        break;
                    case CHILD_UPDATED:
                        LOGGER.info("Worker_" + ID + ":CHILD_UPDATED : " + data.getPath() + "  数据:" + new String(data.getData()));
                        workerElection(Worker.this.client, datasource, workers);
                        break;
                    case CHILD_REMOVED:
                        LOGGER.info("Worker_" + ID + ":CHILD_REMOVED : " + data.getPath());
                        String leaderPath = STORE_ROOT + LEADERS_PATH + "/" + datasource;
                        removeZNode(Worker.this.client, leaderPath);
                        break;
                    default:
                        break;
                }
            }
        };
        childrenCache.getListenable().addListener(childrenCacheListener);
        childrenCache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
    }
}
