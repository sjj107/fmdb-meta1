package com.fiberhome.fmdb.meta.factory;

import com.fiberhome.fmdb.meta.tool.IFMDBMetaClient;

import java.util.Properties;

/**
 * Created by sjj on 19/10/16
 * 工厂类，用于生产元数据客户端
 */
public interface Factory {
    /**
     * 通过默认配置获取元数据客户端
     *
     * @return 元数据客户端
     */
    IFMDBMetaClient getMetaClient();

    IFMDBMetaClient getMetaClient(Properties properties);

}
