package com.fiberhome.fmdb.manifest.monitor;

import org.apache.commons.io.monitor.FileAlterationListener;
import org.apache.commons.io.monitor.FileAlterationMonitor;
import org.apache.commons.io.monitor.FileAlterationObserver;

import java.io.File;

@Deprecated
public class OrcMetaMonitor {
    FileAlterationMonitor monitor = null;

    /**
     * 监控时间间隔
     * @param interval 时间间隔
     */
    public OrcMetaMonitor(long interval) {
        monitor = new FileAlterationMonitor(interval);
    }

    /**
     * 给文件添加监听
     * @param path 监听的文件路径
     * @param listener 监听器
     */
    public void monitor(String path, FileAlterationListener listener) {
        FileAlterationObserver observer = new FileAlterationObserver(new File(path));
        monitor.addObserver(observer);
        observer.addListener(listener);
    }

    public void stop() throws Exception {
        monitor.stop();
    }

    public void start() throws Exception {
        monitor.start();
    }
}
