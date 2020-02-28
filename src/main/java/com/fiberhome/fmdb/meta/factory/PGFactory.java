package com.fiberhome.fmdb.meta.factory;

import com.fiberhome.fmdb.meta.tool.IFMDBMetaClient;
import com.fiberhome.fmdb.meta.tool.impl.PGFMDBMetaClient;

import java.util.Properties;

/**
 * @Description PG客户端的工厂
 * @Author sjj
 * @Date 19/10/31 下午 04:50
 **/
public class PGFactory implements Factory {
    @Override
    public IFMDBMetaClient getMetaClient() {
        return new PGFMDBMetaClient();
    }

    @Override
    public IFMDBMetaClient getMetaClient(Properties properties) {
        return new PGFMDBMetaClient(properties.getProperty("ip"), Integer.parseInt(properties.getProperty("port")), properties.getProperty("data"), properties.getProperty("user"), properties.getProperty("psw"));
    }
}
