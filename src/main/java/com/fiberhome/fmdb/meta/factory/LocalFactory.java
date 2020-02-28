package com.fiberhome.fmdb.meta.factory;

import com.fiberhome.fmdb.meta.tool.IFMDBMetaClient;
import com.fiberhome.fmdb.meta.tool.impl.LocalFMDBMetaClient;
import org.apache.commons.lang.StringUtils;

import java.util.Properties;

/**
 * Created by sjj on 19/10/16
 * 本地工厂，将元数据信息存储到本地
 */
public class LocalFactory implements Factory {

    @Override
    public IFMDBMetaClient getMetaClient() {
        return new LocalFMDBMetaClient();
    }

    @Override
    public IFMDBMetaClient getMetaClient(Properties properties) {
        String jsonDir = properties.getProperty("jsonDir");
        if (StringUtils.isEmpty(jsonDir)) {
            return new LocalFMDBMetaClient();
        } else {
            return new LocalFMDBMetaClient(jsonDir);
        }

    }

}
