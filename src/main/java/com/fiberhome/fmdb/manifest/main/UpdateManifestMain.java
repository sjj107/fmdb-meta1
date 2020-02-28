package com.fiberhome.fmdb.manifest.main;

import com.fiberhome.fmdb.meta.bean.MetaStorageLocation;
import com.fiberhome.fmdb.meta.conf.FMDBMetaConf;
import com.fiberhome.fmdb.meta.factory.FmdbMetaFactory;
import com.fiberhome.fmdb.meta.tool.IFMDBMetaClient;
import com.fiberhome.fmdb.quartz.manager.JobSchedulerManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Set;

/**
 * @Description 更新manifest
 * @Author sjj
 * @Date 19/10/31 下午 08:13
 **/
@Deprecated
public class UpdateManifestMain {
    private static Logger logger = LoggerFactory.getLogger("fmdbquartz");

    //当前已经记录了哪些orc文件的元数据信息，dbName_tableName_orcName
//    public static Set<String> currentInfo = Sets.newHashSet();
    //数据目录
    public static File dataDir;
    //manifest文件存放在哪里
    public static MetaStorageLocation metaStorageLocation = MetaStorageLocation.PG;

    public static IFMDBMetaClient metaClient;

    //更新的周期
    private static String crond;

    public static void main(String[] args) {
//        LoadConfFile.loadLog4j(Constant.LOG4J_CONF_PATH);
        initConf();
        metaClient = FmdbMetaFactory.INSTANCE.getMetaClient(metaStorageLocation);
        JobSchedulerManager jobSchedulerManager = new JobSchedulerManager();
//        currentInfo.addAll(getCurrentManifestInfo());
        logger.debug("跟新manifes的crond为[{}]", crond);
        jobSchedulerManager.startUpdateManifest(crond, null);
    }

    /**
     * 初始化配置文件
     */
    private static void initConf() {
        dataDir = new File(FMDBMetaConf.getInstance().dataDir);
//        crond = FMDBMetaConf.getInstance().manifestCrond;
        metaStorageLocation = MetaStorageLocation.valueOf(FMDBMetaConf.getInstance().metaLocation);
    }

    /**
     * 获取当前manifest的元数据信息已经包含了哪些orc文件
     *
     * @return dbName_tableName_orcName的列表
     */
    private static Set<String> getCurrentManifestInfo() {
//        metaClient = FmdbMetaFactory.INSTANCE.getMetaClient(metaStorageLocation);
        return metaClient.getCurrentOrcFiles();
    }
}
