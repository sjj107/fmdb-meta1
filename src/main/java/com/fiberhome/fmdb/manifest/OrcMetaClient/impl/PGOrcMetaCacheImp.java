package com.fiberhome.fmdb.manifest.OrcMetaClient.impl;

import com.fiberhome.fmdb.common.CommonUtil;
import com.fiberhome.fmdb.common.Constant;
import com.fiberhome.fmdb.common.SQLConstant;
import com.fiberhome.fmdb.manifest.OrcMetaClient.IOrcMetaCache;
import com.fiberhome.fmdb.manifest.bean.DataRange;
import com.fiberhome.fmdb.manifest.bean.ManifestInfo;
import com.fiberhome.fmdb.manifest.bean.OrcColInfo;
import com.fiberhome.fmdb.manifest.bean.TableManifestInfo;
import com.fiberhome.fmdb.manifest.monitor.ICallback;
import com.fiberhome.fmdb.meta.conf.FMDBMetaConf;
import com.fiberhome.fmdb.quartz.manager.JobSchedulerManager;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;
import org.quartz.JobDataMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * @Description orc元数据缓存信息的PG库实现方式
 * @Author sjj
 * @Date 19/11/04 下午 08:33
 **/
public class PGOrcMetaCacheImp implements IOrcMetaCache {
    private static Logger logger = LoggerFactory.getLogger("fmdbmeta");

    //pg的ip信息
    private String pg_ip;
    //pg的端口号信息
    private int pg_port;
    private String pg_user;
    private String pg_psw;
    private String manifestcacheCrond;

    //pg数据存放位置
    private String pg_dataBase;
    //连接池
    private DataSource dataSource;
//    private Connection connection;
//    private PreparedStatement pstmt;

    private JobSchedulerManager jobSchedulerManager = new JobSchedulerManager();

    public PGOrcMetaCacheImp() {
        initConf();
//        initConection();
        initDataSource();
        loadAllManifest();
        startReloadCacheJob();
    }

    public PGOrcMetaCacheImp(String pg_ip, int pg_port, String pg_user, String pg_psw, String pg_dataBase) {
        this.pg_ip = pg_ip;
        this.pg_port = pg_port;
        this.pg_user = pg_user;
        this.pg_psw = pg_psw;
        this.pg_dataBase = pg_dataBase;
//        initConection();
        initDataSource();
        loadAllManifest();
        startReloadCacheJob();
    }

    private void startReloadCacheJob() {
        JobDataMap jobDataMap = new JobDataMap();
        jobDataMap.put("cache", this);
        jobSchedulerManager.startLoadManifest(this.manifestcacheCrond, jobDataMap);
    }

    private void initConf() {
        this.pg_ip = FMDBMetaConf.getInstance().pg_ip;
        this.pg_port = FMDBMetaConf.getInstance().pg_port;
        this.pg_dataBase = FMDBMetaConf.getInstance().pg_dataBase;
        this.pg_user = FMDBMetaConf.getInstance().pg_user;
        this.pg_psw = FMDBMetaConf.getInstance().pg_psw;
        this.manifestcacheCrond = FMDBMetaConf.getInstance().manifestcacheCrond;
    }

//    /**
//     * 初始化操作
//     */
//    private void initConection() {
//        connection = CommonUtil.INSTANCE.getPGConnection(pg_ip, pg_port, pg_dataBase, pg_user, pg_psw);
//    }

    /**
     * 初始化操作
     */
    private void initDataSource() {
        this.dataSource = CommonUtil.INSTANCE.getDataSource(Constant.PG_DRIVER_CLASS, Constant.PG_JDBC_URL_PRE + pg_ip + Constant.COLON + pg_port + pg_dataBase, pg_user, pg_psw);
    }

    /**
     * manifest的缓存信息<br>
     * <库名::表名，TableManifestInfo>
     */
    LoadingCache<String, TableManifestInfo> manifestInfoCache =
            CacheBuilder.newBuilder().maximumSize(500).expireAfterAccess(10, TimeUnit.MINUTES)
                    .build(new CacheLoader<String, TableManifestInfo>() {
                        @Override
                        public TableManifestInfo load(String s) throws Exception {
                            return loadTableManifestInfo(s);
                        }
                    });

    private TableManifestInfo loadTableManifestInfo(String db_tableName) {
        String[] split = db_tableName.split(Constant.DB_TBL_CON);
        if (split.length != 2) {
            logger.error("传入值[{}]格式不对", db_tableName);
            return null;
        }
        TableManifestInfo result = new TableManifestInfo(split[0], split[1]);
        Connection connection = null;
        PreparedStatement stmt = null;
        ResultSet resultSet = null;
        ManifestInfo manifestInfo = null;
        Set<String> parts = Sets.newHashSet();//表下所有的分区名
        try {
            connection = dataSource.getConnection();
            stmt = connection.prepareStatement(SQLConstant.SELECT_MANIFEST_ORCNAME_SQL);
            stmt.setString(1, split[0]);
            stmt.setString(2, split[1]);
            resultSet = stmt.executeQuery();
            OrcColInfo orcColInfo = null;
            while (resultSet.next()) {
//                String partName = resultSet.getString(1);
//                String orcName = resultSet.getString(2);
//                String colName = resultSet.getString(3);
//                String min = resultSet.getString(4);
//                String max = resultSet.getString(5);
//                long notnullnum = resultSet.getLong(6);
//                boolean hasnull = resultSet.getBoolean(7);
//                long nullnum = resultSet.getLong(8);
                orcColInfo = new OrcColInfo(split[0], split[1], resultSet.getString(1), resultSet.getString(2), resultSet.getString(3));
                orcColInfo.setMin(resultSet.getString(4));
                orcColInfo.setMax(resultSet.getString(5));
                orcColInfo.setCount(resultSet.getLong(6));
                orcColInfo.setHasNull(resultSet.getBoolean(7));
                orcColInfo.setNullCount(resultSet.getLong(8));
                result.addOrcColInfo(orcColInfo);
            }
        } catch (SQLException e) {
            logger.error("查询出错", e);
        } finally {
            try {
                if (resultSet != null) {
                    resultSet.close();
                }
                if (stmt != null) {
                    stmt.close();
                }
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException e) {
                logger.error("关闭错误", e);
            }
        }
        return result;
    }

    /**
     * 加载所有的manifest信息
     */
    private void loadAllManifest() {
        String dbName;
        String tableName;
        String key;
        Connection connection = null;
        PreparedStatement pstmt = null;
        ResultSet resultSet = null;
        try {
            connection = this.dataSource.getConnection();
            pstmt = connection.prepareStatement(SQLConstant.SELECT_DB_TBL_NAME);
            resultSet = pstmt.executeQuery();
            while (resultSet.next()) {
                dbName = resultSet.getString(1);
                tableName = resultSet.getString(2);
                key = dbName + Constant.DB_TBL_CON + tableName;
                TableManifestInfo tableManifestInfo = loadTableManifestInfo(key);
                manifestInfoCache.put(key, tableManifestInfo);
            }
        } catch (SQLException e) {
            logger.error("执行SQL[{}]出错", SQLConstant.SELECT_MANIFEST_SQL);
        } finally {
            try {
                if (resultSet != null) {
                    resultSet.close();
                }
                if (pstmt != null) {
                    pstmt.close();
                }
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException e) {
                logger.error("关闭流连接失败");
            }
        }
    }


//    @Override
//    public List<String> getOrcFile(String dbName, String tableName, String colName, String start, String end) {
//        return null;
//    }

//    @Override
//    public Map<String, DataRange> getColumnRange(String dbName, String tableName, String colName) {
//        Map<String, DataRange> result = Maps.newHashMap();
//        try {
//            Table<String, String, DataRange> table = manifestCache.get(dbName + Constant.DB_TBL_CON + tableName);
//            Map<String, DataRange> row = table.row(colName);
//            if (row == null) {
//                logger.warn("变[{}]-[{}]不存在字段[{}]", dbName, tableName, colName);
//                return result;
//            }
//            result.putAll(row);
//        } catch (ExecutionException e) {
//            logger.error("error", e);
//            return result;
//        }
//        return result;
//    }

    @Override
    public TableManifestInfo getManifestInfo(String dbName, String tableName) {
        return manifestInfoCache.getUnchecked(dbName + Constant.DB_TBL_CON + tableName);
    }

    @Override
    public void reloadAllOrcCache() {
        logger.debug("reload 缓存信息...");
        manifestInfoCache.invalidateAll();
        loadAllManifest();
        logger.debug("reload 缓存信息 完成");
    }

//    @Override
//    public long getTableCount(String dbName, String tableName) {
//        return 0;
//    }

    @Override
    public void registMonitor(ICallback callback, int interval) {

    }
}
