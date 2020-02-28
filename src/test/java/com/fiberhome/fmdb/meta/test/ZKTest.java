package com.fiberhome.fmdb.meta.test;

import com.fiberhome.fmdb.zookeeper.ICallback;
import com.fiberhome.fmdb.zookeeper.NodeChildrenChangedWatcher;
import com.fiberhome.fmdb.zookeeper.NodeDataChangeWatcher;
import com.fiberhome.fmdb.zookeeper.ZKManager;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.List;

/**
 * @Description zk测试类
 * @Author sjj
 * @Date 19/12/17 下午 04:20
 **/
public class ZKTest {
    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        ZKManager zookeeper = ZKManager.getInstance();
        NodeDataChangeWatcher watcher = new NodeDataChangeWatcher();
//        zookeeper.createPNode("/fmdb", "data".getBytes());
//        zookeeper.createPersistentMultiPath("/fh/sjj/dd","sjj".getBytes());
//        zookeeper.deleteMultiNode("/fh");
        List<String> children = zookeeper.getChildren("/", null);
        System.out.println(children);
        String data = zookeeper.getData("/FMDB/deleted/table/fhorc/http_4500w_20191217_day", watcher);
        zookeeper.registerZKMonitor("/FMDB/deleted/table", new ICallback() {
            @Override
            public void callback(CheckUpdate checkUpdate) {
                if (checkUpdate == CheckUpdate.NEW) {
                    System.out.println("更新");
                }
            }
        }, new NodeChildrenChangedWatcher());
//        System.out.println(data);
    }
}
