package com.fiberhome.fmdb.zookeeper;

import org.apache.commons.lang.StringUtils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;

import java.util.concurrent.CountDownLatch;

/**
 * @Description 监控db状态
 * @Author sjj
 * @Date 19/12/18 上午 11:06
 **/
public class ZKDBMonitor implements Runnable {
    /**
     * 监控的路径
     */
    private String path;

    /**
     * 回调函数
     */
    private ICallback callback;
    private ZKManager zkManager;
    private Watcher watcher;

    public ZKDBMonitor(String path, ICallback callback, ZKManager zkManager, Watcher watcher) {
        this.path = path;
        this.callback = callback;
        this.zkManager = zkManager;
        this.watcher = watcher;
    }

    @Override
    public void run() {
        while (true) {
            if (StringUtils.isEmpty(path)) {
                break;
            } else {
                try {
//                    zkManager.getData(path, watcher);
                    zkManager.getChildren(path, watcher);
                    ZKEnvs.getStatusLatch().await();
                } catch (InterruptedException | KeeperException e) {
                    Thread.currentThread().interrupt();
                }
                callback.callback(ICallback.CheckUpdate.NEW);
                ZKEnvs.setStatusLatch(new CountDownLatch(1));
            }
        }
    }
}
