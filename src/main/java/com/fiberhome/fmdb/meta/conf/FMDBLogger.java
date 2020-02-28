package com.fiberhome.fmdb.meta.conf;

import com.fiberhome.fmdb.common.Constant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by sjj on 19/10/17
 * FMDB日志类
 */
public class FMDBLogger {
    private static FMDBLogger fmdbLogger;
    private Logger logger = LoggerFactory.getLogger(Constant.FMDB_META_LOG_NAME);

    public Logger getLogger() {
        return logger;
    }

    public void setLogger(Logger logger) {
        this.logger = logger;
    }

    public static FMDBLogger getInstance() {
        if (fmdbLogger == null) {
            fmdbLogger = new FMDBLogger();
        }
        return fmdbLogger;
    }
}
