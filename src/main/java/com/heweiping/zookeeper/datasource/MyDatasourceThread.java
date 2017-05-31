package com.heweiping.zookeeper.datasource;

import com.heweiping.zookeeper.util.Datasource;

/**
 * Created by WeiPing He on 2017/5/31 下午12:37.
 * Email: weiping_he@hansight.com
 */
public class MyDatasourceThread extends Thread {
    private Datasource datasource = null;

    @Override
    public void run() {
        try {
            datasource.startDatasource();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    void buildDatasource(String id, String Workers) throws Exception {
        Datasource datasource = new Datasource(id);
        datasource.saveDataSource(Workers);
        this.datasource = datasource;
    }
}
