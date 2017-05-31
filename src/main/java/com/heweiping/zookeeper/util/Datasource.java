package com.heweiping.zookeeper.util;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.heweiping.zookeeper.util.CuratorUtils.*;

public class Datasource {

    private static final Logger LOGGER = LoggerFactory.getLogger(Datasource.class);

    private CuratorFramework framework = getCuratorFramework();
    private String datasourceName = "datasource_1";

    private PathChildrenCache workerPathCache = null;
    private PathChildrenCache leaderPathCache = null;


    public Datasource(String id) throws Exception {
        this.datasourceName = "datasource_" + id;
    }

    public String getDatasourceName() {
        return this.datasourceName;
    }

    /**
     * 保存数据源
     *
     * @param workers 被指派的worker，以逗号分隔
     */
    public void saveDataSource(String workers) throws Exception {
        createZNodeWithData(framework, STORE_ROOT + DATASOURCES_PATH + "/" + datasourceName, workers.getBytes());
    }

    /**
     * 启动数据源
     */
    public void startDatasource() throws Exception {

        Stat pathStat = framework.checkExists().forPath(STORE_ROOT + LEADERS_PATH + "/" + datasourceName);
        if (pathStat == null) {
            workerElection(framework, datasourceName);
        }

        //1 判断/running/datasources目录是否存在,不存在则创建
        Stat stat = framework.checkExists().forPath(RUNNING_ROOT + DATASOURCES_PATH);
        if (stat == null) {
            framework.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(RUNNING_ROOT + DATASOURCES_PATH);
        }

        //2 如果/running/datasources目录中有当前数据源的信息，删除
        String datasourceRunningPath = RUNNING_ROOT + DATASOURCES_PATH + "/" + datasourceName;
        Stat datasourceRunningState = framework.checkExists().forPath(datasourceRunningPath);
        if (datasourceRunningState != null) {
            removeZNode(framework, datasourceRunningPath);
        }
        //3 加载/store/leaders/中选举出来的Leader Worker
        byte[] data = framework.getData().forPath(STORE_ROOT + LEADERS_PATH + "/" + datasourceName);
        //4 创建Worker运行时目录（Session断开表示该Worker失联，对应目录会自动消失）
        framework.create().withMode(CreateMode.PERSISTENT).forPath(datasourceRunningPath, data);
        LOGGER.info("datasource [{}] is started ,the leader worker is [{}]", datasourceName, new String(framework.getData().forPath(datasourceRunningPath)));

        setWorkerListener(framework);//监控Worker的运行状态
        setLeaderListener(framework);//监控自己的Leader变化
        Thread.sleep(Integer.MAX_VALUE);
    }

    /**
     * 停止数据源
     */
    public void stopDatasource() throws Exception {
//        this.workerPathCache.clear();
        this.workerPathCache.close();
        this.leaderPathCache.close();
//        LOGGER.info("datasource [{}] is stoped", datasourceName);
//        String datasourceRunningPath = RUNNING_ROOT + DATASOURCES_PATH + "/" + datasourceName;
//        Stat datasourceRunningState = framework.checkExists().forPath(datasourceRunningPath);
//        if (datasourceRunningState != null) {
//            removeZNode(framework, datasourceRunningPath);
//        }
        Thread.interrupted();
    }

    /**
     * 对Worker的监控
     *
     * @param client
     * @throws Exception
     */
    private void setWorkerListener(CuratorFramework client) throws Exception {
        PathChildrenCache childrenCache = new PathChildrenCache(client, RUNNING_ROOT + WORKERS_PATH, true);
        PathChildrenCacheListener childrenCacheListener = new PathChildrenCacheListener() {
            @Override
            public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
                ChildData data = event.getData();

                switch (event.getType()) {
                    case CHILD_ADDED:
                        LOGGER.info(datasourceName + ":CHILD_ADDED : " + data.getPath() + "  数据:" + new String(data.getData()));
                        List<String> affectDatasource1 = getAffectDatasources(data.getPath(), STORE_ROOT + DATASOURCES_PATH);
                        if (affectDatasource1 != null && affectDatasource1.size() > 0) {
                            for (String datasource : affectDatasource1) {
                                workerElection(framework, datasource);
                            }
                        }
                        break;
                    case CHILD_REMOVED:
                        LOGGER.info(datasourceName + ":CHILD_REMOVED : " + data.getPath());
                        List<String> affectDatasource2 = getAffectDatasources(data.getPath(), STORE_ROOT + LEADERS_PATH);
                        if (affectDatasource2 != null && affectDatasource2.size() > 0) {
                            for (String datasource : affectDatasource2) {
                                removeZNode(framework, STORE_ROOT + LEADERS_PATH + "/" + datasource);
                            }
                        }
                        List<String> affectDatasource3 = getAffectDatasources(data.getPath(), STORE_ROOT + DATASOURCES_PATH);
                        if (affectDatasource3 != null && affectDatasource3.size() > 0) {
                            for (String datasource : affectDatasource3) {
                                workerElection(framework, datasource);
                            }
                        }
                        break;
                    default:
                        break;
                }
            }

            private List<String> getAffectDatasources(String workerPath, String datasourcePath) throws Exception {
                List<String> tmp = Arrays.asList(workerPath.split("/"));
                String changedWorker = tmp.get(tmp.size() - 1);
                Map<String, List<String>> workers = getWorkerToDatasourceMap(framework, datasourcePath, null);
                List<String> affectDatasource = new ArrayList<String>();
                for (Map.Entry<String, List<String>> entry : workers.entrySet()) {
                    if (entry.getKey().contains(changedWorker) || entry.getKey().equals(changedWorker)) {
                        affectDatasource.addAll(entry.getValue());
                    }
                }
                return affectDatasource;
            }
        };
        childrenCache.getListenable().addListener(childrenCacheListener);
        childrenCache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
        this.workerPathCache = childrenCache;
    }

    private void setLeaderListener(CuratorFramework client) throws Exception {
        PathChildrenCache childrenCache = new PathChildrenCache(client, STORE_ROOT + LEADERS_PATH, true);
        PathChildrenCacheListener childrenCacheListener = new PathChildrenCacheListener() {
            @Override
            public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
                ChildData data = event.getData();
                //排除不是本数据源的leader变化
                if (data != null && data.getPath().contains(datasourceName)) {
                    List<String> tmp = Arrays.asList(data.getPath().split("/"));
                    String datasourceName = tmp.get(tmp.size() - 1);
                    switch (event.getType()) {
                        case CHILD_ADDED:
                            LOGGER.info("CHILD_ADDED : " + datasourceName + "  数据:" + new String(data.getData()));
                            Thread.sleep(2000);
                            stopDatasource();
                            startDatasource();
                            break;
                        case CHILD_REMOVED:
                            LOGGER.info("CHILD_REMOVED : " + datasourceName);
                            stopDatasource();
                            break;
                        case CHILD_UPDATED:
                            LOGGER.info("CHILD_UPDATED : " + datasourceName + "  数据:" + new String(data.getData()));
                            Thread.sleep(2000);
                            stopDatasource();
                            startDatasource();
                            break;
                        default:
                            break;
                    }
                }
            }
        };
        childrenCache.getListenable().addListener(childrenCacheListener);
        childrenCache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
        this.leaderPathCache = childrenCache;
    }
}