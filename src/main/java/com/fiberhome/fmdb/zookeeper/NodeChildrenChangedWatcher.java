package com.fiberhome.fmdb.zookeeper;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

/**
 * @Description 监控子节点的变化
 * @Author sjj
 * @Date 19/12/18 上午 10:56
 **/
public class NodeChildrenChangedWatcher implements Watcher {
    @Override
    public void process(WatchedEvent event) {
        if (event.getType() == Event.EventType.NodeChildrenChanged) {
            String path = event.getPath();
            System.out.println(path + "changed...");
        }
    }
}
