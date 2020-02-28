package com.fiberhome.fmdb.meta.factory;

import com.fiberhome.fmdb.meta.bean.MetaStorageLocation;
import com.fiberhome.fmdb.meta.conf.FMDBMetaConf;
import com.fiberhome.fmdb.meta.tool.IFMDBMetaClient;
import com.fiberhome.fmdb.meta.tool.impl.LocalFMDBMetaClient;
import com.fiberhome.fmdb.meta.tool.impl.PGFMDBMetaClient;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by sjj on 19/10/28
 * fmdb元数据工厂
 */
public enum FmdbMetaFactory {
    INSTANCE;
    private static Logger logger = LoggerFactory.getLogger("fmdbmeta");

    private  static IFMDBMetaClient pgfmdbmetaclient= new PGFMDBMetaClient();


    /**
     * 根据配置文件获取元数据客户端
     *
     * @return 元数据客户端
     */
    public IFMDBMetaClient getMetaClient() {
        if (StringUtils.isEmpty(FMDBMetaConf.getInstance().metaLocation)) {
            logger.debug("配置文件未设置FMDB元数据的存放位置，默认存在本地");
            return new LocalFMDBMetaClient();
        }
        MetaStorageLocation location = MetaStorageLocation.valueOf(FMDBMetaConf.getInstance().metaLocation.toUpperCase());
        return getMetaClient(location);
    }

    public IFMDBMetaClient getMetaClient(MetaStorageLocation location) {
        switch (location) {
//            case LOCAL:
//                logger.debug("FMDB元数据存放在本地");
//                return new LocalFMDBMetaClient();
            case PG:
                logger.debug("FMDB元数据存放在PG");
                return  pgfmdbmetaclient;
            default:
                return null;
        }
    }

}
