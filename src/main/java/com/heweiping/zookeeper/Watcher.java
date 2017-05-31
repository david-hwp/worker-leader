package com.heweiping.zookeeper;

import com.heweiping.zookeeper.util.CuratorUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by WeiPing He on 2017/5/31 上午10:33.
 * Email: weiping_he@hansight.com
 */
public class Watcher {
    public static void main(String[] args) {
        CuratorFramework client = CuratorUtils.getCuratorFramework();
        try {
            setListenerThreeThree(client, "/running/workers");
            setListenerThreeThree(client, "/running/datasources");
            setListenerThreeThree(client, "/store/leaders");
//            setListenerThreeThree(client, "/store/datasources");
            Thread.sleep(Long.MAX_VALUE);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private static void setListenerThreeThree(CuratorFramework client, final String path) throws Exception {
        ExecutorService pool = Executors.newCachedThreadPool();
        //设置节点的cache
        TreeCache treeCache = new TreeCache(client, path);
        //设置监听器和处理过程
        treeCache.getListenable().addListener(new TreeCacheListener() {
            @Override
            public void childEvent(CuratorFramework client, TreeCacheEvent event) throws Exception {
                ChildData data = event.getData();
                if (data != null && !data.getPath().contains("lock")) {
                    switch (event.getType()) {
                        case NODE_ADDED:
                            System.out.println("NODE_ADDED : " + data.getPath() + "  数据:" + new String(data.getData()));
                            break;
                        case NODE_REMOVED:
                            System.out.println("NODE_REMOVED : " + data.getPath());
                            break;
                        case NODE_UPDATED:
                            System.out.println("NODE_UPDATED : " + data.getPath() + "  数据:" + new String(data.getData()));
                            break;
                        default:
                            System.out.println("UNHANDLED EVENT : " + event.getType());
                            break;
                    }
                    showChildsAndData(client, path);
                }
            }
        });
        //开始监听
        treeCache.start();
    }

    private synchronized static void showChildsAndData(CuratorFramework client, String path) throws Exception {
        List<String> childs = client.getChildren().forPath(path);
        for (String child : childs) {
            String childPath = path + "/" + child;
            System.out.println(childPath + "<------" + new String(client.getData().forPath(childPath)));
        }
    }
}
