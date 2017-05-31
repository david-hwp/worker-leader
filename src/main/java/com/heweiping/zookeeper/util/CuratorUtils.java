package com.heweiping.zookeeper.util;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.ACLProvider;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Created by WeiPing He on 2017/5/28 下午8:11.
 * Email: weiping_he@hansight.com
 */
public class CuratorUtils {

    public static final String RUNNING_ROOT = "/running";//运行时环境，存储正在运行的worker和datasource
    public static final String STORE_ROOT = "/store";//持久化存储位置
    public static final String WORKERS_PATH = "/workers";//保存正在运行的work信息，worker失联该目录下的数据会改变
    public static final String DATASOURCES_PATH = "/datasources";//数据源保存位置，保存数据源和被指定的几个Worker之间的关系
    public static final String LEADERS_PATH = "/leaders";//Leaders保存位置，保存数据源和所负责采集的Leader Worker之间的关系

    private static final Logger LOGGER = LoggerFactory.getLogger(Worker.class);

    private static String connectString = "127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183";
    private static RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);

    /**
     * 获得一个新的curator 客户端
     *
     * @return
     */
    public static CuratorFramework getCuratorFramework() {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        CuratorFramework client = CuratorFrameworkFactory.newClient(connectString, retryPolicy);
        client.start();
        return client;
    }

    public static CuratorFramework getCuratorFrameworkWithAuth() {
        //默认创建的根节点是没有做权限控制的--需要自己手动加权限???----
        ACLProvider aclProvider = new ACLProvider() {
            private List<ACL> acl;

            @Override
            public List<ACL> getDefaultAcl() {
                if (acl == null) {
                    ArrayList<ACL> acl = ZooDefs.Ids.CREATOR_ALL_ACL;
                    acl.clear();
                    acl.add(new ACL(ZooDefs.Perms.ALL, new Id("auth", "admin:admin")));
                    this.acl = acl;
                }
                return acl;
            }

            @Override
            public List<ACL> getAclForPath(String path) {
                return acl;
            }
        };
        String scheme = "digest";
        byte[] auth = "admin:admin".getBytes();
        int connectionTimeoutMs = 5000;
        String namespace = "testnamespace";
        CuratorFramework client = CuratorFrameworkFactory.builder().aclProvider(aclProvider).
                authorization(scheme, auth).
                connectionTimeoutMs(connectionTimeoutMs).
                connectString(connectString).
                namespace(namespace).
                retryPolicy(new RetryNTimes(Integer.MAX_VALUE, 1000)).build();
        client.start();
        return client;
    }

    /**
     * 获得正在运行的Worker节点信息
     *
     * @param client curator 客户端
     * @return
     * @throws Exception
     */
    public static List<String> getRunningWorkers(CuratorFramework client) throws Exception {
        return client.getChildren().forPath(RUNNING_ROOT + WORKERS_PATH);
    }

    /**
     * 指定一个数据源重新选举出一个Leader Worker
     *
     * @param client     curator 客户端
     * @param datasource 需要选择Leader的数据源
     */
    public static void workerElection(CuratorFramework client, String datasource) throws Exception {
        String workerString = new String(client.getData().forPath(STORE_ROOT + DATASOURCES_PATH + "/" + datasource));
        List<String> workers = Arrays.asList(workerString.split(","));
        workerElection(client, datasource, workers);

    }

    /**
     * Worker Election 从数据源所选择的Worker列表中选举出一个成为Leader Worker，并存放在/store/leaders/目录下
     *
     * @param client     curator 客户端
     * @param datasource 需要选择Leader的数据源
     * @param workers    被选择的Worker列表
     */
    public static void workerElection(CuratorFramework client, String datasource, List<String> workers) throws Exception {
        //从数据源指定的worker中挑出正在运行的worker
        List<String> runningWorkers = getRunningWorkers(client);
        List<String> usefulWorkers = new ArrayList<String>();
        for (String worker : workers) {
            for (String runningWorker : runningWorkers) {
                if (runningWorker.equals(worker)) {
                    usefulWorkers.add(worker);
                }
            }
        }

        if (usefulWorkers.size() == 0) {
            throw new Exception("no worker is running for datasource: " + datasource);
        } else if (usefulWorkers.size() == 1) {
            String leaderPath = STORE_ROOT + LEADERS_PATH + "/" + datasource;
            createZNodeWithData(client, leaderPath, usefulWorkers.get(0).getBytes());
        } else {
//            checkZNode(client, RUNNING_ROOT + DATASOURCES_PATH);
            checkZNode(client, STORE_ROOT + LEADERS_PATH);

            //获得每个worker充当了几个datasource的Leader
//            Map<String, List<String>> workerLeaders = getWorkerToDatasourceMap(client, RUNNING_ROOT + DATASOURCES_PATH, datasource);
            Map<String, List<String>> workerLeaders = getWorkerToDatasourceMap(client, STORE_ROOT + LEADERS_PATH, datasource);

            //选择lead了最少datasource的worker为当前datasource的leader
            String leaderWorker = "";
            int minDatasource = -1;
            for (String worker : usefulWorkers) {
                if (workerLeaders.get(worker) == null) {
                    leaderWorker = worker;
                    minDatasource = 0;
                    continue;
                }
                if (minDatasource == -1 || minDatasource > workerLeaders.get(worker).size()) {
                    leaderWorker = worker;
                    minDatasource = workerLeaders.get(worker).size();
                }
            }
            String leaderPath = STORE_ROOT + LEADERS_PATH + "/" + datasource;
            createZNodeWithData(client, leaderPath, leaderWorker.getBytes());
        }
    }

    /**
     * 将datasource to worker的对应关系，转化为worker to datasource的对应关系，用于统计每个worker充当了几个datasource的Leader
     *
     * @param client     curator 客户端
     * @param datasource 需要排除的数据源名
     */
    public static Map<String, List<String>> getWorkerToDatasourceMap(CuratorFramework client, String path, String datasource) throws Exception {
        List<String> childPaths = client.getChildren().forPath(path);
        Map<String, List<String>> workerLeaders = new HashMap<String, List<String>>();
        for (String child : childPaths) {
            if (client.checkExists().forPath(path + "/" + child) != null) {
                String workerId = new String(client.getData().forPath(path + "/" + child));
                if (!child.equals(datasource)) {
                    if (workerLeaders.containsKey(workerId)) {
                        workerLeaders.get(workerId).add(child);
                    } else {
                        List<String> datasources = new ArrayList<String>();
                        datasources.add(child);
                        workerLeaders.put(workerId, datasources);
                    }
                }
            }
        }
        return workerLeaders;
    }

    /**
     * 删除一个Leader Worker
     *
     * @param client curator 客户端
     * @param path   要删除的路径
     */
    public static void removeZNode(CuratorFramework client, String path) throws Exception {

        client.delete().inBackground(new BackgroundCallback() {
            @Override
            public void processResult(CuratorFramework curatorFramework, CuratorEvent curatorEvent) throws Exception {
                LOGGER.debug("zk node path[{}] is deleted", curatorEvent.getPath());
            }
        }).forPath(path);
    }

    /**
     * 创建zookeeper路径并比对
     *
     * @param client curator 客户端
     * @param path   路径
     * @param data   最新版本的数据
     */
    public static void createZNodeWithData(CuratorFramework client, String path, byte[] data) throws Exception {
        InterProcessMutex sharedLock = new InterProcessMutex(client, path);
        try {
            //获取到分布式共享锁
            if (sharedLock.acquire(50, TimeUnit.MILLISECONDS)) {
                Stat stat = client.checkExists().forPath(path);
                if (stat == null) {
                    client.create().creatingParentsIfNeeded().forPath(path, data);
                    LOGGER.debug("path [{}] has been created ,the data is [{}]", path, new String(data));
                } else {
                    byte[] oldData = client.getData().forPath(path);
                    if (!Arrays.equals(oldData, data)) {
                        client.setData().forPath(path, data);
                        LOGGER.debug("path {} is already exists but the data [{}] is not same, update it!", path, new String(data));
                    } else {
                        LOGGER.debug("path {} is already exists and the data is same ", path);
                    }
                }
            }
        } catch (Exception e) {
            //日志记录一下 超时说明 有锁 可以不再操作
            LOGGER.error("get zookeeper sharedLock failed :\n{}", e);
        } finally {
            try {
                if (sharedLock.isAcquiredInThisProcess()) {
                    //其共享所持有锁，进行锁释放的操作
                    sharedLock.release();
                    LOGGER.debug("the sharedLock has been released.");
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 检查对应的路径在zookeeper中是否存在，不存在即创建
     *
     * @param client curator 客户端
     * @param path   被检查的路径
     */
    public static void checkZNode(CuratorFramework client, String path) throws Exception {
        if (client.checkExists().forPath(path) == null) {
            client.create().creatingParentsIfNeeded().forPath(path);
            LOGGER.debug("path [{}] has been created.", path);
        } else {
            LOGGER.debug("path {} is already exists.", path);
        }
    }
}
