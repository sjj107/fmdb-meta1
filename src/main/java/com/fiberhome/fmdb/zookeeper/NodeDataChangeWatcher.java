package com.fiberhome.fmdb.zookeeper;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

/**
 * @Description 监控节点变化
 * @Author sjj
 * @Date 19/12/18 上午 11:19
 **/
public class NodeDataChangeWatcher implements Watcher {
    @Override
    public void process(WatchedEvent event) {
        if (event.getType() == Event.EventType.NodeDataChanged) {
            ZKEnvs.getStatusLatch().countDown();
        }
    }
}
