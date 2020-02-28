package com.fiberhome.fmdb.manifest.monitor;

import org.apache.commons.io.monitor.FileAlterationListener;
import org.apache.commons.io.monitor.FileAlterationObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

@Deprecated
public class OrcMetaListener implements FileAlterationListener {
    private static Logger logger = LoggerFactory.getLogger("orcmeta");

    private ICallback  callback;

    public OrcMetaListener(ICallback callback) {
        this.callback = callback;
    }

    @Override
    public void onStart(FileAlterationObserver observer) {
        logger.debug("开始监控");
    }

    @Override
    public void onDirectoryCreate(File directory) {
        logger.info("[{}]目录创建",directory.getName());
        this.callback.callback(ICallback.CheckUpdate.NEW);
    }

    @Override
    public void onDirectoryChange(File directory) {
        logger.info("[{}]目录变化",directory.getName());
    }

    @Override
    public void onDirectoryDelete(File directory) {
        logger.info("[{}]目录删除",directory.getName());
    }

    @Override
    public void onFileCreate(File file) {
        logger.info("[{}]文件创建",file.getName());
        this.callback.callback(ICallback.CheckUpdate.NEW);
    }

    @Override
    public void onFileChange(File file) {
        logger.info("[{}]文件变化",file.getName());
        this.callback.callback(ICallback.CheckUpdate.NEW);
    }

    @Override
    public void onFileDelete(File file) {
        logger.info("[{}]文件删除",file.getName());
    }

    @Override
    public void onStop(FileAlterationObserver observer) {
        logger.debug("监控停止");
    }
}
