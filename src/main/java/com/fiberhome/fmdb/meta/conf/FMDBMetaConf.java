package com.fiberhome.fmdb.meta.conf;

import com.fiberhome.fmdb.common.CommonUtil;
import com.fiberhome.fmdb.common.Constant;
import com.fiberhome.fmdb.common.LoadConfFile;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

/**
 * @Description 解析配置文件
 * @Author sjj
 * @Date 19/11/02 下午 01:32
 **/
public class FMDBMetaConf {
    private static Logger logger = LoggerFactory.getLogger("fmdbmeta");
    private volatile static FMDBMetaConf instance;
    /**
     * 不包括base目录
     */
    public String FMDBDataDir;
    /**
     * 包括base目录
     */
    public String dataDir;
    public String metaLocation;
//    public String manifestCrond;
    public String manifestcacheCrond;
    //    public String jsonDir;
//    public String h2_ip;
//    public int h2_port;
//    public String h2_dataDir;
    public String pg_ip;
    public int pg_port;
    public String pg_dataBase;
    public String pg_user;
    public String pg_psw;
    public String zk_url;
//    public int orc_flush_interval;
    //依赖的第三方包路径
    public String external_jars_dir;

    private FMDBMetaConf() {
        init();
    }


    /**
     * 初始化配置
     */
    private void init() {
        try {
            Properties properties = LoadConfFile.load(Constant.FMDB_CONF_PATH);
            String dataDirC = properties.getProperty("dataDir");
            if (StringUtils.isEmpty(dataDirC)) {
                logger.warn("配置文件[{}]未设置[{}]配置项,采用默认值[{}]", Constant.FMDB_CONF_PATH, "dataDir", CommonUtil.INSTANCE.getSystemName().contains("Windows") ? Constant.DEFAULT_DATA_DIR_WIN : Constant.DEFAULT_DATA_DIR_LINUX);
                FMDBDataDir = CommonUtil.INSTANCE.getSystemName().contains("Windows") ? Constant.DEFAULT_DATA_DIR_WIN : Constant.DEFAULT_DATA_DIR_LINUX;
                dataDir = FMDBDataDir + Constant.SLASH + Constant.DEFAULT_DATA_BASE;
            } else {
                FMDBDataDir = dataDirC;
                dataDir = FMDBDataDir + Constant.SLASH + Constant.DEFAULT_DATA_BASE;
            }
            String metaLocationC = properties.getProperty("metaLocation");
            if (StringUtils.isEmpty(metaLocationC)) {
                logger.warn("配置文件[{}]未设置[{}]配置项,采用默认值[{}]", Constant.FMDB_CONF_PATH, "metaLocation", Constant.DEFAULT_META_LOCATION);
                metaLocation = Constant.DEFAULT_META_LOCATION;
            } else {
                metaLocation = metaLocationC;
            }
//            String manifestCrondC = properties.getProperty("manifestCrond");
//            if (StringUtils.isEmpty(manifestCrondC)) {
//                logger.warn("配置文件[{}]未设置[{}]配置项,采用默认值[{}]", Constant.FMDB_CONF_PATH, "manifestCrond", Constant.DEFAULT_MANIFEST_CROND);
//                manifestCrond = Constant.DEFAULT_MANIFEST_CROND;
//            } else {
//                manifestCrond = manifestCrondC;
//            }
            String manifestcacheCrondC = properties.getProperty("manifestcacheCrond");
            if (StringUtils.isEmpty(manifestcacheCrondC)) {
                logger.warn("配置文件[{}]未设置[{}]配置项,采用默认值[{}]", Constant.FMDB_CONF_PATH, "manifestcacheCrond", Constant.DEFAULT_MANIFESTCACHE_CROND);
                manifestcacheCrond = Constant.DEFAULT_MANIFESTCACHE_CROND;
            } else {
                manifestcacheCrond = manifestcacheCrondC;
            }
//            if (metaLocation.equalsIgnoreCase("local")) {
//                String jsonDirC = properties.getProperty("jsonDir");
//                if (StringUtils.isEmpty(jsonDirC)) {
//                    logger.warn("配置文件[{}]未设置[{}]配置项,采用默认值[{}]", Constant.FMDB_CONF_PATH, "jsonDir", CommonUtil.INSTANCE.getSystemName().contains("Windows") ? Constant.DEFAULT_JSON__DIR_WIN : Constant.DEFAULT_JSON_DIR_LINUX);
//                    jsonDir = CommonUtil.INSTANCE.getSystemName().contains("Windows") ? Constant.DEFAULT_JSON__DIR_WIN : Constant.DEFAULT_JSON_DIR_LINUX;
//                } else {
//                    jsonDir = jsonDirC;
//                }
//            } else if (metaLocation.equalsIgnoreCase("h2"))
//            {
//                String h2_ipC = properties.getProperty("h2_ip");
//                if (StringUtils.isEmpty(h2_ipC)) {
//                    logger.warn("配置文件[{}]未设置[{}]配置项,采用默认值[{}]", Constant.FMDB_CONF_PATH, "h2_ip", Constant.H2_IP);
//                    h2_ip = Constant.H2_IP;
//                } else {
//                    h2_ip = h2_ipC;
//                }
//                String h2_portC = properties.getProperty("h2_port");
//                if (StringUtils.isEmpty(h2_portC)) {
//                    logger.warn("配置文件[{}]未设置[{}]配置项,采用默认值[{}]", Constant.FMDB_CONF_PATH, "h2_port", Constant.H2_PORT);
//                    h2_port = Constant.H2_PORT;
//                } else {
//                    try {
//                        h2_port = Integer.parseInt(h2_portC);
//                    } catch (NumberFormatException e) {
//                        logger.warn("配置文件[{}]-[{}]配置项格式错误,采用默认值[{}]", Constant.FMDB_CONF_PATH, "h2_port", Constant.H2_PORT);
//                        h2_port = Constant.H2_PORT;
//                    }
//                }
//                String h2_dataDirC = properties.getProperty("h2_dataDir");
//                if (StringUtils.isEmpty(h2_dataDirC)) {
//                    logger.warn("配置文件[{}]未设置[{}]配置项,采用默认值[{}]", Constant.FMDB_CONF_PATH, "h2_dataDir", Constant.H2_DATA_DIR);
//                    h2_dataDir = Constant.H2_DATA_DIR;
//                } else {
//                    h2_dataDir = h2_dataDirC;
//                }
//            } else
            if (metaLocation.equalsIgnoreCase("pg")) {
                String pg_ipC = properties.getProperty("pg_ip");
                if (StringUtils.isEmpty(pg_ipC)) {
                    logger.warn("配置文件[{}]未设置[{}]配置项,采用默认值[{}]", Constant.FMDB_CONF_PATH, "pg_ip", Constant.PG_IP);
                    pg_ip = Constant.PG_IP;
                } else {
                    pg_ip = pg_ipC;
                }
                String pg_portC = properties.getProperty("pg_port");
                if (StringUtils.isEmpty(pg_portC)) {
                    logger.warn("配置文件[{}]未设置[{}]配置项,采用默认值[{}]", Constant.FMDB_CONF_PATH, "pg_port", Constant.PG_PORT);
                    pg_port = Constant.PG_PORT;
                } else {
                    try {
                        pg_port = Integer.parseInt(pg_portC);
                    } catch (NumberFormatException e) {
                        logger.warn("配置文件[{}]-[{}]配置项格式错误,采用默认值[{}]", Constant.FMDB_CONF_PATH, "pg_port", Constant.PG_PORT);
                        pg_port = Constant.PG_PORT;
                    }
                }
                String pg_dataBaseC = properties.getProperty("pg_dataBase");
                if (StringUtils.isEmpty(pg_dataBaseC)) {
                    logger.warn("配置文件[{}]未设置[{}]配置项,采用默认值[{}]", Constant.FMDB_CONF_PATH, "pg_dataBase", Constant.PG_DATABASE);
                    pg_dataBase = Constant.PG_DATABASE;
                } else {
                    pg_dataBase = pg_dataBaseC;
                }
                String pg_userC = properties.getProperty("pg_user");
                if (StringUtils.isEmpty(pg_userC)) {
                    logger.warn("配置文件[{}]未设置[{}]配置项,采用默认值[{}]", Constant.FMDB_CONF_PATH, "pg_user", Constant.PG_USER);
                    pg_user = Constant.PG_USER;
                } else {
                    pg_user = pg_userC;
                }
                String pg_pswC = properties.getProperty("pg_psw");
                if (StringUtils.isEmpty(pg_pswC)) {
                    logger.warn("配置文件[{}]未设置[{}]配置项,采用默认值[{}]", Constant.FMDB_CONF_PATH, "pg_psw", Constant.PG_PSW);
                    pg_psw = Constant.PG_PSW;
                } else {
                    pg_psw = pg_pswC;
                }

            } else {
                logger.error("元数据信息目前只支持pg");
            }
            String zk_urlC = properties.getProperty("zk_url");
            if (StringUtils.isEmpty(zk_urlC)) {
                logger.warn("配置文件[{}]未设置[{}]配置项,采用默认值[{}]", Constant.FMDB_CONF_PATH, "zk_url", Constant.ZK_URL);
                zk_url = Constant.ZK_URL;
            } else {
                zk_url = zk_urlC;
            }
            String external_jars_dirC = properties.getProperty("external_jars_dir");
            if (StringUtils.isEmpty(external_jars_dirC)) {
                logger.warn("配置文件[{}]未设置[{}]配置项,采用默认值[{}]", Constant.FMDB_CONF_PATH, "external_jars_dir", Constant.EXTERNAL_JARS_DIR);
                external_jars_dir = Constant.EXTERNAL_JARS_DIR;
            } else {
                external_jars_dir = external_jars_dirC;
            }
//            String orc_flush_intervalC = properties.getProperty("orc_flush_interval");
//            if (StringUtils.isEmpty(orc_flush_intervalC)) {
//                logger.warn("配置文件[{}]未设置[{}]配置项,采用默认值[{}]", Constant.FMDB_CONF_PATH, "orc_flush_interval", Constant.ORC_FLUSH_INTERVAL);
//                orc_flush_interval = Constant.ORC_FLUSH_INTERVAL;
//            } else {
//                try {
//                    orc_flush_interval = Integer.parseInt(orc_flush_intervalC);
//                } catch (NumberFormatException e) {
//                    logger.warn("配置文件[{}]-[{}]配置项格式错误,采用默认值[{}]", Constant.FMDB_CONF_PATH, "orc_flush_interval", Constant.ORC_FLUSH_INTERVAL);
//                    orc_flush_interval = Constant.ORC_FLUSH_INTERVAL;
//                }
//            }
        } catch (IOException e) {
            logger.error("读取配置文件[{}]出错", Constant.FMDB_CONF_PATH);
        }
    }

    public static FMDBMetaConf getInstance() {
        if (instance == null) {
            synchronized (FMDBMetaConf.class) {
                if (instance == null) {
                    instance = new FMDBMetaConf();
                }
            }
        }
        return instance;
    }
}
