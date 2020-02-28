package com.fiberhome.fmdb.manifest.factory;

import com.fiberhome.fmdb.manifest.OrcMetaClient.IOrcMetaCache;
import com.fiberhome.fmdb.manifest.OrcMetaClient.impl.LocalOrcMetaCacheImp;
import com.fiberhome.fmdb.manifest.OrcMetaClient.impl.PGOrcMetaCacheImp;
import com.fiberhome.fmdb.meta.bean.MetaStorageLocation;
import com.fiberhome.fmdb.meta.conf.FMDBMetaConf;
import com.fiberhome.fmdb.meta.tool.impl.LocalFMDBMetaClient;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Description orc元数据缓存工厂
 * @Author sjj
 * @Date 19/11/05 上午 08:43
 **/
public enum OrcMetaCacheFactory {
    INSTANCE;
    private static Logger logger = LoggerFactory.getLogger("fmdbmeta");

    public IOrcMetaCache getOrcMetaCache() {
        if (StringUtils.isEmpty(FMDBMetaConf.getInstance().metaLocation)) {
            logger.debug("配置文件未设置FMDB元数据的存放位置，默认存在PG");
            return new PGOrcMetaCacheImp();
        }
        MetaStorageLocation location = MetaStorageLocation.valueOf(FMDBMetaConf.getInstance().metaLocation.toUpperCase());
        return getOrcMetaCache(location);
    }

    public IOrcMetaCache getOrcMetaCache(MetaStorageLocation location) {
        switch (location) {
//            case LOCAL:
//                return new LocalOrcMetaCacheImp();
            default:
                return new PGOrcMetaCacheImp();
        }
    }
}
