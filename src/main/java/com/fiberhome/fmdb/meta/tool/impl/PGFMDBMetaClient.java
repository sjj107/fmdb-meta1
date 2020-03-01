package com.fiberhome.fmdb.meta.tool.impl;

import com.alibaba.fastjson.JSON;
import com.fiberhome.fmdb.common.Constant;
import com.fiberhome.fmdb.common.JarLoader;
import com.fiberhome.fmdb.manifest.bean.OrcInfo;
import com.fiberhome.fmdb.meta.bean.*;
import com.fiberhome.fmdb.meta.conf.FMDBMetaConf;
import com.fiberhome.fmdb.meta.tool.IFMDBMetaClient;
import com.fiberhome.fmdb.statistic.ORCUtil;
import com.fiberhome.fmdb.zookeeper.ZKManager;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.orc.OrcConf;
import org.apache.orc.UndefineEncodingBean;
import org.apache.orc.UndefineEncodingJsonObj;
import org.apache.orc.util.ORCUDCTRegister;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @Description PG元数据客户端
 * @Author sjj
 * @Date 19/10/31 下午 03:36
 **/
public class PGFMDBMetaClient implements IFMDBMetaClient {
    /**
     * 日志
     */
    private static Logger logger = LoggerFactory.getLogger("fmdbmeta");
    //pg的ip信息
    private String pg_ip;
    //pg的端口号信息
    private int pg_port;
    private String pg_user;
    private String pg_psw;

    //pg数据存放位置
    private String pg_dataBase;

    private PGClient pgClient;
    private ZKManager zkManager;

    private static  volatile  Configuration conf=new Configuration();

    LoadingCache<String, TableInfo> tableCache = CacheBuilder.newBuilder().maximumSize(500)/*.expireAfterAccess(10, TimeUnit.MINUTES)*/
            .build(new CacheLoader<String, TableInfo>() {
                @Override
                public TableInfo load(String s) throws Exception {
                    return loadTableInfo(s);
                }
            });
    LoadingCache<String, IndexInfo> indexCache = CacheBuilder.newBuilder().maximumSize(500)/*.expireAfterAccess(10, TimeUnit.MINUTES)*/
            .build(new CacheLoader<String, IndexInfo>() {
                @Override
                public IndexInfo load(String s) throws Exception {
                    return loadIndexInfo(s);
                }
            });
    //FMDB支持的数据类型
    LoadingCache<String, UDCTInfo> fmdbColTypeCache = CacheBuilder.newBuilder().maximumSize(500)/*.expireAfterAccess(10, TimeUnit.MINUTES)*/
            .build(new CacheLoader<String, UDCTInfo>() {
                @Override
                public UDCTInfo load(String s) throws Exception {
                    return loadColTypeCache(s);
                }
            });

    private UDCTInfo loadColTypeCache(String s) {
        return pgClient.getUDCTInfo(s);
    }

    private IndexInfo loadIndexInfo(String s) {
        String[] split = s.split(Constant.DB_TBL_CON);
        return pgClient.getIndexInfo(split[0], split[1]);
    }

    private TableInfo loadTableInfo(String s) {
        String[] split = s.split(Constant.DB_TBL_CON);
        return pgClient.getTableInfo(split[0], split[1]);
    }

    public PGFMDBMetaClient(String pg_ip, int pg_port, String pg_dataBase, String pg_user, String pg_psw) {
        this.pg_ip = pg_ip;
        this.pg_port = pg_port;
        this.pg_dataBase = pg_dataBase;
        this.pg_user = pg_user;
        this.pg_psw = pg_psw;
        init();
//        initCache();
        initZK();
        loadUDCT();
        //loadJars();
        //this.conf.set(OrcConf.ORC_ENCODE_IMPL.getAttribute(),this.getUDCTConfJson());
    }


    public PGFMDBMetaClient() {
        initConf();
        init();
//        initCache();
        initZK();
        loadUDCT();
        //loadJars();
        //this.conf.set(OrcConf.ORC_ENCODE_IMPL.getAttribute(),this.getUDCTConfJson());
    }

    private void loadUDCT(){
        ORCUDCTRegister.registEnum(new ArrayList<>(this.getAllUDCTNames()));
        this.conf.set(OrcConf.ORC_ENCODE_IMPL.getAttribute(),this.getUDCTConfJson());
        this.loadJars();
    }

    /**
     * 加载依赖的jar包
     */
    private boolean loadJars(UDCTInfo udctInfo) {
        logger.debug("开始加载第三方依赖的jar包...");
        String external_jars_dir = FMDBMetaConf.getInstance().external_jars_dir;
        if (!new File(external_jars_dir).exists()) {
            logger.error("目录[{}]不存在", external_jars_dir);
            return false;
        }
        try {
            List<String> classNameList=JarLoader.loadAllJarFromAbsolute(external_jars_dir);

           if(classNameList.contains(udctInfo.getRead())&&classNameList.contains(udctInfo.getWriter())){
               return true;
           }


        } catch (IOException e) {
            logger.error("加载第三方的[{}]jar包失败", external_jars_dir, e);
        } catch (NoSuchMethodException e) {
            logger.error("加载第三方的[{}]jar包失败", external_jars_dir, e);
        } catch (IllegalAccessException e) {
            logger.error("加载第三方的[{}]jar包失败", external_jars_dir, e);
        } catch (InvocationTargetException e) {
            logger.error("加载第三方的[{}]jar包失败", external_jars_dir, e);
        }
        logger.debug("加载第三方依赖的jar包完成。");

        return  false;
    }


    private void loadJars() {
        logger.debug("开始加载第三方依赖的jar包...");
        String external_jars_dir = FMDBMetaConf.getInstance().external_jars_dir;
        if (!new File(external_jars_dir).exists()) {
            logger.error("目录[{}]不存在", external_jars_dir);
        }
        try {
           JarLoader.loadAllJarFromAbsolute(external_jars_dir);


        } catch (IOException e) {
            logger.error("加载第三方的[{}]jar包失败", external_jars_dir, e);
        } catch (NoSuchMethodException e) {
            logger.error("加载第三方的[{}]jar包失败", external_jars_dir, e);
        } catch (IllegalAccessException e) {
            logger.error("加载第三方的[{}]jar包失败", external_jars_dir, e);
        } catch (InvocationTargetException e) {
            logger.error("加载第三方的[{}]jar包失败", external_jars_dir, e);
        }
        logger.debug("加载第三方依赖的jar包完成。");

    }



    private void initZK() {
        try {
            zkManager = ZKManager.getInstance();
        } catch (Exception e) {
            logger.error("连接zk失败", e);
            System.exit(1);
        }
    }

    /**
     * 初始化所有缓存信息
     */
    private void initCache() {
        initMetaCache();
        List<UDCTInfo> allUDCTInfo = pgClient.getAllUDCTInfo();
        for (UDCTInfo udctInfo : allUDCTInfo) {
            fmdbColTypeCache.put(udctInfo.getUdct_name(), udctInfo);
        }
    }
    /**
     * 初始化meta缓存信息
     */
    private  void  initMetaCache(){
        List<String> allDBNames = pgClient.getAllDBNames();
        for (String dbName : allDBNames) {
            List<String> allTableNames = pgClient.getAllTableName(dbName);
            for (String tableName : allTableNames) {
                TableInfo tableInfo = pgClient.getTableInfo(dbName, tableName);
                if (tableInfo != null) {
                    tableCache.put(dbName + Constant.DB_TBL_CON + tableName, pgClient.getTableInfo(dbName, tableName));
                    List<String> allIndexName = pgClient.getAllIndexName(dbName, tableName);
                    for (String indexName : allIndexName) {
                        indexCache.put(dbName + Constant.DB_TBL_CON + indexName, pgClient.getIndexInfo(dbName, indexName));
                    }
                }
            }
        }
    }




    /**
     * 初始化配置
     */
    private void initConf() {
        this.pg_ip = FMDBMetaConf.getInstance().pg_ip;
        this.pg_port = FMDBMetaConf.getInstance().pg_port;
        this.pg_dataBase = FMDBMetaConf.getInstance().pg_dataBase;
        this.pg_user = FMDBMetaConf.getInstance().pg_user;
        this.pg_psw = FMDBMetaConf.getInstance().pg_psw;
    }

    /**
     * 初始化操作
     */
    private void init() {
        pgClient = PGClient.getInstance(pg_ip, pg_port, pg_user, pg_psw, pg_dataBase);
    }

    @Override
    public List<Category> getCategories() {
        List<Category> categories = Lists.newArrayList();
        Category category1 = new Category(true,"INT");
        Category category2 = new Category(false,"STRUCT");
        categories.add(category1);
        categories.add(category2);
        return categories;
    }

    @Override
    public boolean creatUDCT(UDCTInfo udctInfo) {

        boolean b=this.loadJars(udctInfo);
        if(b) {
            b = pgClient.creatUDCT(udctInfo);
            if (b) {
                fmdbColTypeCache.put(udctInfo.getUdct_name(), udctInfo);
            }
            List<String> new_udct = new ArrayList<>();
            new_udct.add(udctInfo.getUdct_name());
            ORCUDCTRegister.registEnum(new_udct);
            this.conf.set(OrcConf.ORC_ENCODE_IMPL.getAttribute(), this.getUDCTConfJson());
        }

        return b;
    }

    @Override
    public boolean deleteUDCT(String udctName) {
        boolean b = pgClient.deleteUDCT(udctName);
        if (b) {
            fmdbColTypeCache.invalidate(udctName.toLowerCase());
        }
        return b;
    }

    @Override
    public UDCTInfo getUDCTInfo(String udctName) {
        UDCTInfo result = null;
        try {
            result = fmdbColTypeCache.get(udctName.toLowerCase());
        } catch (Exception e) {
            logger.error("获取[{}]自定义数据类型失败", udctName);
        }
        return result;
    }

    @Override
    public Set<String> getAllUDCTNames() {
        return fmdbColTypeCache.asMap().keySet();
    }

    @Override
    public boolean creatDB(String dbName) {
        try {
            while (zkManager.exists(Constant.DELETE_DB_ZK + dbName)) {
                logger.debug("merge为取消未完成，等1s");
                Thread.sleep(1000);
            }
        } catch (KeeperException e) {
            logger.error("查询zk节点[{}]失败", Constant.DELETE_DB_ZK + dbName, e);
        } catch (InterruptedException e) {
            logger.error("查询zk节点[{}]失败", Constant.DELETE_DB_ZK + dbName, e);
        }
        return pgClient.creatDB(dbName);
    }

    @Override
    public boolean deleteDB(String dbName) {
        boolean b = pgClient.deleteDB(dbName);
        if (b) {
            //在ZK上注册信息
            zkManager.createPersistentMultiPath(Constant.DELETE_DB_ZK + dbName, "deleted".getBytes());
        }
        return b;
    }

    @Override
    public boolean storageTable(TableInfo tableInfo) {
        try {
            while (zkManager.exists(Constant.DELETE_TBL_ZK + tableInfo.getDbName() + Constant.DB_TBL_CON + tableInfo.getTableName())) {
                logger.debug("merge为取消未完成，等1s");
                Thread.sleep(1000);
            }
        } catch (KeeperException e) {
            logger.error("查询zk节点[{}]失败", Constant.DELETE_TBL_ZK + tableInfo.getDbName() + Constant.DB_TBL_CON + tableInfo.getTableName(), e);
            return false;
        } catch (InterruptedException e) {
            logger.error("查询zk节点[{}]失败", Constant.DELETE_TBL_ZK + tableInfo.getDbName() + Constant.DB_TBL_CON + tableInfo.getTableName(), e);
            return false;
        }
        boolean b = pgClient.storageTable(tableInfo);
        if (b) {
            //需要反查pg库，完善tableInfo中字段的basetype信息
            tableCache.put(tableInfo.getDbName() + Constant.DB_TBL_CON + tableInfo.getTableName(), pgClient.getTableInfo(tableInfo.getDbName(), tableInfo.getTableName()));
        }
        return b;
    }

    @Override
    public boolean storageIndex(IndexInfo indexInfo) {
        boolean b = pgClient.storageIndex(indexInfo);
        if (b) {
            indexCache.put(indexInfo.getDbName() + Constant.DB_TBL_CON + indexInfo.getIndexName(), indexInfo);
        }
        return b;
    }

    @Override
    public boolean deleteTable(String dbName, String tableName) {
        TableInfo tableInfo = null;
        try {
            tableInfo = tableCache.get(dbName + Constant.DB_TBL_CON + tableName);
        } catch (Exception e) {
            logger.info("不存在表[{}]-[{}],无需删除", dbName, tableName);
            return true;
        }
        List<String> allIndexName = pgClient.getAllIndexName(dbName, tableName);
        boolean b = pgClient.deleteTable(dbName, tableName);
        if (b) {
            tableCache.invalidate(dbName + Constant.DB_TBL_CON + tableName);
            for (String indexname : allIndexName) {
                indexCache.invalidate(dbName + Constant.DB_TBL_CON + indexname);
            }
        }
//        try {
//            FileUtils.deleteDirectory(new File(new File(FMDBMetaConf.getInstance().dataDir, dbName), tableName));
//        } catch (IOException e) {
//            logger.error("删除原始数据失败", e);
//        }
        //在zk上注册删表的信息:dbName::tableName
        if (b) {
            zkManager.createPersistentMultiPath(Constant.DELETE_TBL_ZK + dbName + Constant.DB_TBL_CON + tableName, "deleted".getBytes());
        }
        return b;
    }

    @Override
    public boolean truncateTable(String dbName, String tableName) {
        boolean b = pgClient.truncateTable(dbName, tableName);
        if (b) {
            zkManager.createPersistentMultiPath(Constant.TRUNCATE_TBL_ZK + dbName + Constant.DB_TBL_CON + tableName, "deleted".getBytes());
        }
        return b;
    }

    @Override
    public boolean deleteIndex(String dbName, String tableName, String indexName) {
        boolean b = pgClient.deleteIndex(dbName, tableName, indexName);
        if (b) {
            indexCache.invalidate(dbName + Constant.DB_TBL_CON + indexName);
        }
        return b;
    }

    @Override
    public Set<String> getAllDBNames() {
        Set<String> strings = tableCache.asMap().keySet();
        Set<String> result = Sets.newHashSet();
        for (String s : strings) {
            result.add(s.split(Constant.DB_TBL_CON)[0]);
        }
        return result;
    }

    @Override
    public List<String> getAllTableName(String dbName) {
        Set<String> strings = tableCache.asMap().keySet();
        List<String> result = Lists.newArrayList();
        for (String s : strings) {
            if (s.startsWith(dbName + Constant.DB_TBL_CON)) {
                result.add(s.split(Constant.DB_TBL_CON)[1]);
            }
        }
        return result;
    }

    @Override
    public List<String> getAllIndexName(String dbName, String tableName) {
        TableInfo tableInfo = getTableInfo(dbName, tableName);
        List<String> result = Lists.newArrayList();
        if (tableInfo == null) {
            logger.error("不存在表[{}]-[{}]", dbName, tableName);
            return result;
        }
        List<IndexInfo> indexs = tableInfo.getIndexs();
        for (IndexInfo indexInfo : indexs) {
            result.add(indexInfo.getIndexName());
        }
        return result;
    }

    @Override
    public TableInfo getTableInfo(String dbName, String tableName) {
        TableInfo result = null;
        try {
            result = tableCache.get(dbName + Constant.DB_TBL_CON + tableName);
        } catch (Exception e) {
            return result;
        }
        return result;
    }

    @Override
    public IndexInfo getIndexInfo(String dbName, String indexName) {
        try {
            return indexCache.get(dbName + Constant.DB_TBL_CON + indexName);
        } catch (Exception e) {
            return null;
        }
    }

    @Override
    public String getColType(String dbName, String tableName, String colName) {
        TableInfo tableInfo = getTableInfo(dbName, tableName);
        if (tableInfo == null) {
            logger.error("不存在表[{}]-[{}]", dbName, tableName);
            return null;
        }
        ColumnInfo columnInfo = tableInfo.getColumnInfo(colName);
        if (columnInfo == null) {
            logger.error("表[{}]-[{}]不存在字段[{}]", dbName, tableName, colName);
            return null;
        }
        return columnInfo.getBaseType().getDesc();
    }

    @Override
    public boolean storageOrcInfo(String dataDir, String dbName, String tableName, String partName, String orcName, boolean stat) {
        OrcInfo orcInfo = ORCUtil.INSTANCE.getOrcinfo(dataDir, dbName, tableName, partName, orcName);
        return pgClient.storageOrcInfo(orcInfo, stat);
    }

    @Override
    public boolean storageOrcInfo(OrcInfo orcInfo, boolean stat) {
        return pgClient.storageOrcInfo(orcInfo, stat);
    }

    /**
     * 存储表的orc元数据信息
     *
     * @param dbName    库名
     * @param tableName 表名
     * @return
     */
    public boolean storageTableManifestInfo(String dbName, String tableName) {
        File tableDir = new File(new File(FMDBMetaConf.getInstance().dataDir, dbName), tableName);
        if (!tableDir.exists() || !tableDir.isDirectory()) {
            logger.error("不存在[{}]-[{}]表数据目录", dbName, tableName);
            return false;
        }
        File[] partDirs = tableDir.listFiles();
        if (partDirs.length == 0) {
            logger.error("[{}]-[{}]表不存在分区目录", dbName, tableName);
            return false;
        }
        for (File partDir : partDirs) {

        }
        return true;
    }

    @Override
    public Set<String> getCurrentOrcFiles() {
        return pgClient.getCurrentOrcFiles();
    }

    @Override
    public boolean deleteTableManifest(String dbName, String tableName) {
        return pgClient.deleteTableManifest(dbName, tableName);
    }

    @Override
    public boolean changeOrcManifestStat(List<OrcInfo> falseL, List<OrcInfo> trueL) {
        return pgClient.changeOrcManifestStat(falseL, trueL);
    }

    @Override
    public void close() {
        pgClient.close();
    }

    @Override
    public void reloadCache() {

        List<UDCTInfo> allUDCTInfo = pgClient.getAllUDCTInfo();
        Set<String> cacheAllUDCTNames=new HashSet<>();
        cacheAllUDCTNames.addAll(this.getAllUDCTNames());
        List<String> new_udct=new ArrayList<>();
        for (UDCTInfo udctInfo : allUDCTInfo) {
            try {
                if(!cacheAllUDCTNames.remove(udctInfo.getUdct_name())){
                    new_udct.add(udctInfo.getUdct_name());
                    fmdbColTypeCache.put(udctInfo.getUdct_name(), udctInfo);
                    ORCUDCTRegister.registEnum(new_udct);
                    this.conf.set(OrcConf.ORC_ENCODE_IMPL.getAttribute(),getUDCTConfJson());
                    loadJars();
                }

            } catch (Exception e) {
                  logger.error("reload UDCTInfo 缓存出错"+e);
            }
        }

        tableCache.invalidateAll();
        indexCache.invalidateAll();
        initMetaCache();

        //FMDB支持的数据类型

    }

    @Override
    public String getUDCTConfJson() {
        ArrayList<UndefineEncodingBean> lists = new ArrayList<>();
        Set<String> allUDCTNames = getAllUDCTNames();

        for (String udctName : allUDCTNames) {
            UDCTInfo udctInfo = getUDCTInfo(udctName);
            UndefineEncodingBean undefineEncodingBean = new UndefineEncodingBean(udctInfo.getUdct_name(), udctInfo.getWriter(), udctInfo.getRead(), udctInfo.getBase_type().getDesc());
            lists.add(undefineEncodingBean);
        }
        UndefineEncodingJsonObj undefineEncodingJsonObj = new UndefineEncodingJsonObj();
        undefineEncodingJsonObj.setList(lists);
        String json = JSON.toJSONString(undefineEncodingJsonObj);
        return json;
    }

    @Override
     public Configuration getOrcConfiguration() {
        return conf;
    }
}
