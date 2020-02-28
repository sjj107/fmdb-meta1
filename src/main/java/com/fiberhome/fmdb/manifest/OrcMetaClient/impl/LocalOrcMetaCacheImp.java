package com.fiberhome.fmdb.manifest.OrcMetaClient.impl;

import com.fiberhome.fmdb.common.Constant;
import com.fiberhome.fmdb.common.LoadConfFile;
import com.fiberhome.fmdb.manifest.OrcMetaClient.IOrcMetaCache;
import com.fiberhome.fmdb.manifest.bean.DataRange;
import com.fiberhome.fmdb.manifest.bean.ManifestInfo;
import com.fiberhome.fmdb.manifest.bean.TableManifestInfo;
import com.fiberhome.fmdb.manifest.colcompare.DataRangeCompareFactory;
import com.fiberhome.fmdb.manifest.colcompare.IDataRangeCompare;
import com.fiberhome.fmdb.manifest.monitor.ICallback;
import com.fiberhome.fmdb.manifest.monitor.OrcMetaListener;
import com.fiberhome.fmdb.manifest.monitor.OrcMetaMonitor;
import com.fiberhome.fmdb.meta.conf.FMDBMetaConf;
import com.fiberhome.fmdb.meta.tool.IFMDBMetaClient;
import com.fiberhome.fmdb.meta.tool.impl.LocalFMDBMetaClient;
import com.fiberhome.fmdb.statistic.ORCStatistic;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Created by sjj on 19/10/21
 */
@Deprecated
public class LocalOrcMetaCacheImp implements IOrcMetaCache {
    private static Logger logger = LoggerFactory.getLogger("orcmeta");
    private  IFMDBMetaClient metaClient;
    private  Path path;
    private  Configuration conf = new Configuration();
    private  Reader reader = null;
    private  BufferedReader bufferedReader;
    /**
     * orc数据存放的位置
     */
    private static String dataDir;

    public LocalOrcMetaCacheImp() {
        initConf();
        loadAllOrcMeta();
//        loadTableCountCache();
        uploadCache(new ICallback() {
            @Override
            public void callback(CheckUpdate checkUpdate) {
                if (checkUpdate == CheckUpdate.NEW) {
//                    orcMetaCache.invalidateAll();
//                    tableCountCache.invalidateAll();
                    loadAllOrcMeta();
//                    loadTableCountCache();
                }
            }
        });
    }

    private void initConf() {
        dataDir = FMDBMetaConf.getInstance().pg_dataBase;
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

        return null;
    }

    private  Table<String, DataRange, String> getTable(File manifestFile) {
        Table<String, DataRange, String> table = HashBasedTable.create();
        String line;
        try {
            bufferedReader = new BufferedReader(new FileReader(manifestFile));
            while ((line = bufferedReader.readLine()) != null) {
                String[] split = line.split("\t");
                DataRange dataRange = new DataRange(split[1], split[2]);
                table.put(split[0], dataRange, split[3]);
            }
        } catch (IOException e) {
            logger.error("读取orc元数据文件报错", e);
        }
        return table;
    }

    /**
     * 加载所有的orc元数据信息
     */
    private void loadAllOrcMeta() {
        File file = new File(dataDir);
        if (!file.exists() || (!file.isDirectory())) {
            logger.error("不存在{}文件夹", dataDir);
            return;
        }
        File[] dbfiles = file.listFiles();
        if (dbfiles == null) {
            logger.error("{}不是文件夹", dataDir);
            return;
        }
        //字段名，最小值和最大值，orc文件路径
        Table<String, DataRange, String> table;
        //表名，字段名，最小值和最大值，orc文件路径
        Map<String, Table<String, DataRange, String>> map = Maps.newHashMap();
        //元数据文件中一行的内容
        String line;
        for (File dbfile : dbfiles) {
            if (!dbfile.isDirectory()) {
                continue;
            }
            File[] tableFiles = dbfile.listFiles();
            for (File file1 : tableFiles) {
                if (!file1.isDirectory()) {
                    continue;
                }
                File manifestFile = new File(file1.getAbsolutePath(), "manifest");
                if (!manifestFile.exists()) {
                    logger.error("[{}]下缺少manifest文件", file1.getAbsolutePath());
                    continue;
                }
                table = getTable(manifestFile);
                map.put(file1.getName(), table);
            }
//            orcMetaCache.put(dbfile.getName(), map);


        }
    }

//    @Override
//    public List<String> getOrcFile(String dbName, String tableName, String colName, String start, String end) {
//        List<String> result = Lists.newArrayList();
//        try {
//            Map<String, Table<String, DataRange, String>> stringTableMap = orcMetaCache.get(dbName);
//            if (stringTableMap == null) {
//                return result;
//            }
//            Table<String, DataRange, String> table = stringTableMap.get(tableName);
//            if (table == null) {
//                return result;
//            }
//            String colType = metaClient.getColType(dbName, tableName, colName);
//            if (colType == null) {
//                return result;
//            }
//            Map<DataRange, String> row = table.row(colName);
//            if (start == null && end == null) {
//                logger.warn("最大值和最小值全为null");
//                return Lists.newArrayList(row.values());
//            }
//            Set<DataRange> dataRanges = row.keySet();
//            for (DataRange dataRange : dataRanges) {
//                if (hasUnit(dataRange, colType, start, end)) {
//                    result.add(row.get(dataRange));
//                }
//            }
//        } catch (ExecutionException e) {
//            logger.error("获取信息错误", e);
//        }
//
//        return result;
//    }

//    @Override
//    public Map<String, DataRange> getColumnRange(String dbName, String tableName, String colName) {
//        Map<String, DataRange> result = Maps.newHashMap();
//        try {
//            Map<String, Table<String, DataRange, String>> stringTableMap = orcMetaCache.get(dbName);
//            if (stringTableMap == null) {
//                logger.error("不存在库[{}]", dbName);
//                return null;
//            }
//            Table<String, DataRange, String> colInfo = stringTableMap.get(tableName);
//            if (colInfo == null) {
//                logger.error("不存在表[{}]-[{}]", dbName, tableName);
//                return null;
//            }
//            Map<DataRange, String> row = colInfo.row(colName);
//            if (row == null) {
//                logger.error("不存在字段[{}]-[{}]-[{}]", dbName, tableName, colName);
//                return null;
//            }
//            for (Map.Entry<DataRange, String> entry : row.entrySet()) {
//                result.put(entry.getValue(), entry.getKey());
//            }
//        } catch (ExecutionException e) {
//            logger.error("获取信息失败", e);
//        }
//        return result;
//    }

    @Override
    public TableManifestInfo getManifestInfo(String dbName, String tableName) {
        return null;
    }

    @Override
    public void reloadAllOrcCache() {
        logger.info("开始重新加载orc元数据信息...");
//        orcMetaCache.invalidateAll();
        loadAllOrcMeta();
        logger.info("重新加载orc元数据信息结束");
    }
//
//    @Override
//    public long getTableCount(String dbName, String tableName) {
//        Long result = -1L;
//        try {
//            Map<String, Long> map = tableCountCache.get(dbName);
//            if (map == null) {
//                logger.error("不存在库[{}]", dbName);
//                return -1;
//            }
//            result = map.get(tableName);
//            if (result == null) {
//                logger.error("不存在表[{}]-[{}]", dbName, tableName);
//                return -1;
//            }
//        } catch (ExecutionException e) {
//            logger.error("获取数据失败", e);
//        }
//        return result;
//    }

    @Override
    public void registMonitor(ICallback callback, int interval) {
        OrcMetaMonitor monitor = new OrcMetaMonitor(interval);
        monitor.monitor(dataDir, new OrcMetaListener(callback));
        try {
            monitor.start();
        } catch (Exception e) {
            logger.error("监控失败", e);
        }
    }

    /**
     * 够呛能更新缓存
     */
    private static void uploadCache(ICallback callback) {
        OrcMetaMonitor monitor = new OrcMetaMonitor(Constant.MONITOR_INTERVAL);
        monitor.monitor(dataDir, new OrcMetaListener(callback));
        try {
            monitor.start();
        } catch (Exception e) {
            logger.error("监控失败", e);
        }
    }

    /**
     * 传入的最小值和最大值和orc中的记录是否有交集
     *
     * @param dataRange orc元数据中记录的最小值和最大值
     * @param colType   字段类型
     * @param start     最小值
     * @param end       最大值
     * @return 是否有交集
     */
    private boolean hasUnit(DataRange dataRange, String colType, String start, String end) {
        IDataRangeCompare compare = DataRangeCompareFactory.getCompare(colType);
        if (compare == null) {
            logger.error("字段类型不合法,[{}]", colType);
        }
        return compare.compare(dataRange, start, end);
    }
}
