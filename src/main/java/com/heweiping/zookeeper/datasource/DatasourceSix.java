package com.heweiping.zookeeper.datasource;

/**
 * Created by WeiPing He on 2017/5/31 下午12:38.
 * Email: weiping_he@hansight.com
 */
public class DatasourceSix {
    public static void main(String[] args) throws Exception {
        MyDatasourceThread myDatasourceThread = new MyDatasourceThread();
        myDatasourceThread.buildDatasource("6", "worker_1,worker_2,worker_3");
        myDatasourceThread.start();
    }
}
