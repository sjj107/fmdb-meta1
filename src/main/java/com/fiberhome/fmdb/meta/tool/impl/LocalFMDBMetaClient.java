package com.fiberhome.fmdb.meta.tool.impl;

import com.alibaba.fastjson.JSON;
import com.fiberhome.fmdb.common.Constant;
import com.fiberhome.fmdb.manifest.bean.ManifestInfo;
import com.fiberhome.fmdb.manifest.bean.OrcInfo;
import com.fiberhome.fmdb.meta.bean.ColumnInfo;
import com.fiberhome.fmdb.meta.bean.IndexInfo;
import com.fiberhome.fmdb.meta.bean.TableInfo;
import com.fiberhome.fmdb.meta.bean.UDCTInfo;
import com.fiberhome.fmdb.meta.tool.IFMDBMetaClient;
import com.fiberhome.fmdb.statistic.ORCUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.List;
import java.util.Set;

/**
 * Created by sjj on 19/10/16
 * 用于将fmdb的元数据存储到本地
 */
@Deprecated
public class LocalFMDBMetaClient implements IFMDBMetaClient {
    /**
     * 日志
     */
    private static Logger logger = LoggerFactory.getLogger("fmdbmeta");
    /**
     * json文件存放的位置
     */
    private String jsonDir;

    /**
     * 指定元数据存放的位置
     *
     * @param jsonDir 元数据存放的位置
     */
    public LocalFMDBMetaClient(String jsonDir) {
        this.jsonDir = jsonDir;
        initDir();
    }


    /**
     * 使用默认的元数据存放位置<br>
     * linux:/home/nebula/fmdb/json<br>
     * windows:E:\data\fmdb\json
     */
    public LocalFMDBMetaClient() {
        initConf();
        initDir();
    }

    /**
     * 初始化json文件的目录
     */
    private void initDir() {
        logger.debug("元数据存放位置: [{}]", jsonDir);
        File jsonDir = new File(this.jsonDir);
        //如果不存在db目录，则创建
        if (!jsonDir.exists() || !jsonDir.isDirectory()) {
            boolean mkdirs = jsonDir.mkdirs();
            if (mkdirs) {
                logger.debug("创建table目录成功：" + jsonDir.getAbsolutePath());
            }
        }
    }

    /**
     * 初始化操作
     */
    private void initConf() {
        //初始化json文件的存放位置信息
//        this.jsonDir = FMDBMetaConf.getInstance().jsonDir;
    }

    @Override
    public boolean creatUDCT(UDCTInfo udctInfo) {
        return false;
    }

    @Override
    public boolean deleteUDCT(String udctName) {
        return false;
    }

    @Override
    public UDCTInfo getUDCTInfo(String udctName) {
        return null;
    }

    @Override
    public Set<String> getAllUDCTNames() {
        return null;
    }

    @Override
    public boolean creatDB(String dbName) {
        File dbFile = new File(jsonDir, dbName);
        if (!dbFile.exists() || !dbFile.isDirectory()) {
            boolean mkdirs = dbFile.mkdirs();
            logger.info("创建数据库[{}]" + (mkdirs ? "成功" : "失败"), dbName);
            return mkdirs;
        }
        return true;
    }

    @Override
    public boolean deleteDB(String dbName) {
        File dbFile = new File(jsonDir, dbName);
        if (dbFile.exists() && dbFile.isDirectory()) {
            try {
                FileUtils.deleteDirectory(dbFile);
            } catch (IOException e) {
                logger.info("删除数据库[{}]失败", dbName, e);
                return false;
            }
            logger.info("删除数据库[{}]成功", dbName);
            return true;
        }
        return true;
    }

    @Override
    public boolean storageTable(TableInfo tableInfo) {
        String tableName = tableInfo.getTableName();
        String dbName = tableInfo.getDbName();
        String s = tableInfo2JsonString(tableInfo);
        return writeTableFile(dbName, tableName, s);
    }

    private boolean writeTableFile(String dbName, String tableName, String s) {
        BufferedWriter writer;
        try {
            File tableDir = new File(jsonDir, dbName + Constant.SLASH + tableName);
            if (tableDir.exists() || !tableDir.isDirectory()) {
                tableDir.mkdirs();
            }
            writer = new BufferedWriter(new FileWriter(new File(tableDir, tableName + Constant.TABLE_SUFFIX)));
            writer.write(s);
            writer.flush();
            writer.close();
        } catch (IOException e) {
            logger.error("写数据失败", e);
            return false;
        }
        logger.debug("存储表[{}]-[{}]成功", dbName, tableName);
        return true;
    }

    private String tableInfo2JsonString(TableInfo tableInfo) {
        return JSON.toJSONString(tableInfo);
    }

    @Override
    public boolean storageIndex(IndexInfo indexInfo) {
        String indexName = indexInfo.getIndexName();
        String orgTableName = indexInfo.getOrgTableName();
        String dbName = indexInfo.getDbName();
        String s = indexInfo2JsonString(indexInfo);
        boolean b = writeIndexFile(dbName, orgTableName, indexName, s);
        logger.debug("存储索引[{}]-[{}]-[{}]" + (b ? "成功" : "失败"), dbName, orgTableName, indexName);
        return b;
    }

    private boolean writeIndexFile(String dbName, String orgTableName, String indexName, String s) {
        BufferedWriter writer = null;
        try {
            File tableDir = new File(jsonDir, dbName + Constant.SLASH + orgTableName);
            if (!tableDir.exists() || !tableDir.isDirectory()) {
                logger.error("不存在原始表[{}]-[{}]，不能创建该索引[{}]", dbName, orgTableName, indexName);
                return false;
            }
            writer = new BufferedWriter(new FileWriter(new File(tableDir, indexName + Constant.INDEX_SUFFIX)));
            writer.write(s);
            writer.flush();
            writer.close();
        } catch (IOException e) {
            logger.error("写数据失败", e);
            return false;
        }
        return true;
    }

    private String indexInfo2JsonString(IndexInfo indexInfo) {
        return JSON.toJSONString(indexInfo);
    }

    @Override
    public boolean deleteTable(String dbName, String tableName) {
        File tableDir = new File(jsonDir, dbName + Constant.SLASH + tableName);
        if (!tableDir.exists() || !tableDir.isDirectory()) {
            logger.debug("不存在表[{}]-[{}]", dbName, tableName);
            return true;
        }
        try {
            FileUtils.deleteDirectory(tableDir);
        } catch (IOException e) {
            logger.error("删除表[{}]-[{}]失败", dbName, tableName, e);
            return false;
        }
        deleteTableManifest(dbName, tableName);
        logger.info("删除表[{}]-[{}]成功", dbName, tableName);
        return true;
    }

    @Override
    public boolean truncateTable(String dbName, String tableName) {
        return false;
    }

    @Override
    public boolean deleteIndex(String dbName, String tableName, String indexName) {
        File dbDir = new File(jsonDir, dbName);
        if (!dbDir.exists() || !dbDir.isDirectory()) {
            logger.warn("不存在库[{}]", dbName);
            return true;
        }
        File tableDir = new File(dbDir, tableName);
        if (!tableDir.exists() || !tableDir.isDirectory()) {
            logger.warn("不存在表[{}]-[{}]", dbName, tableName);
            return true;
        }
        File indexFile = new File(tableDir, indexName + Constant.INDEX_SUFFIX);
        if (!indexFile.exists() || indexFile.isDirectory()) {
            logger.debug("不存在索引[{}]-[{}]-[{}]", dbName, tableName, indexName);
            return true;
        }
        deleteTableManifest(dbName, indexName);
        boolean delete = indexFile.delete();
        logger.debug("删除索引[{}]-[{}]-[{}]" + (delete ? "成功" : "失败"), dbName, tableName, indexName);
        return delete;
    }

    @Override
    public Set<String> getAllDBNames() {
        Set<String> result = Sets.newHashSet();
        File file = new File(jsonDir, "table");
        if (!file.exists() || !file.isDirectory()) {
            return result;
        }
        File[] files = file.listFiles();
        for (File file1 : files) {
            result.add(file1.getName());
        }
        return result;
    }

    @Override
    public List<String> getAllTableName(String dbName) {
        List<String> result = Lists.newArrayList();
        File file = new File(jsonDir, dbName);
        if (!file.exists() || !file.isDirectory()) {
            return result;
        }
        File[] files = file.listFiles();
        for (File file1 : files) {
            if (file1.isDirectory()) {
                result.add(file1.getName());
            }
        }
        return result;
    }

    @Override
    public List<String> getAllIndexName(String dbName, String tableName) {
        List<String> result = Lists.newArrayList();
        File dbDir = new File(jsonDir, dbName);
        if (!dbDir.exists() || !dbDir.isDirectory()) {
            logger.error("不存在数据库[{}]", dbName);
            return result;
        }
        File tableDir = new File(dbDir, tableName);
        if (!tableDir.exists() || !tableDir.isDirectory()) {
            logger.error("不存在表[{}]-[{}]", dbName, tableName);
            return result;
        }
        File[] files = tableDir.listFiles(Constant.INDEX_FILTER);
        for (File file : files) {
            result.add(file.getName().split("\\.")[0]);
        }
        return result;
    }

    @Override
    public TableInfo getTableInfo(String dbName, String tableName) {
        File tableDir = new File(jsonDir, dbName + Constant.SLASH + tableName);
        if (!tableDir.exists() || !tableDir.isDirectory()) {
            logger.warn("不存在表[{}]-[{}]", dbName, tableName);
            return null;
        }
        File[] indexFiles = tableDir.listFiles(Constant.INDEX_FILTER);
        List<IndexInfo> indexs = Lists.newArrayList();
        for (File indexFile : indexFiles) {
            indexs.add(parseJson2IndexInfo(indexFile.getAbsolutePath()));
        }
        File file = new File(tableDir, tableName + Constant.TABLE_SUFFIX);
        if (!file.exists() || file.isDirectory()) {
            logger.warn("不存在表[{}]-[{}]", dbName, tableName);
            return null;
        }
        TableInfo tableInfo = parseJson2TableInfo(file.getAbsolutePath());
        tableInfo.setIndexs(indexs);
        return tableInfo;
    }

    /**
     * 将json文件转换成TableInfo对象
     *
     * @param jsonFile json文件路径
     * @return 表结构
     */
    private TableInfo parseJson2TableInfo(String jsonFile) {
        String s = readFile(jsonFile);
        TableInfo tableInfo = JSON.parseObject(s, TableInfo.class);
        return tableInfo;
    }

    /**
     * 将json文件转换成字符串
     *
     * @param jsonFile json文件地址
     * @return json文件内容
     */
    private String readFile(String jsonFile) {
        BufferedReader reader = null;
        String laststr = "";
        try {
            FileInputStream fileInputStream = new FileInputStream(jsonFile);
            InputStreamReader inputStreamReader = new InputStreamReader(fileInputStream);
            reader = new BufferedReader(inputStreamReader);
            String tmpString;
            while ((tmpString = reader.readLine()) != null) {
                laststr += tmpString;
            }
            reader.close();
        } catch (IOException e) {
            logger.error(jsonFile + " 不存在");
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException ex) {
                    ex.printStackTrace();
                }
            }
        }
        return laststr;
    }

    @Override
    public IndexInfo getIndexInfo(String dbName, String indexName) {
        File dbDir = new File(jsonDir, dbName);
        if (!dbDir.exists() || !dbDir.isDirectory()) {
            logger.warn("不存在库[{}]", dbName);
            return null;
        }
        File[] tableDirs = dbDir.listFiles();
        for (File tableDir : tableDirs) {
            if (!tableDir.isDirectory()) {
                continue;
            }
            for (File indexFile : tableDir.listFiles(Constant.INDEX_FILTER)) {
                if (indexFile.getName().equals(indexName + Constant.INDEX_SUFFIX)) {
                    return parseJson2IndexInfo(indexFile.getAbsolutePath());
                }
            }
        }
        logger.warn("不存在索引[{}]-[{}]", dbName, indexName);
        return null;
    }

    @Override
    public String getColType(String dbName, String tableName, String colName) {
        TableInfo tableInfo = getTableInfo(dbName, tableName);
        if (tableInfo == null) {
            logger.error("不存在表[{}]", tableName);
            return null;
        }
        ColumnInfo columnInfo = tableInfo.getColumnInfo(colName);
        if (columnInfo == null) {
            logger.error("表[{}]不存在字段[{}]", tableName, colName);
            throw new IllegalArgumentException("表[" + tableName + "]不存在字段[" + colName + "]");
        }
        return columnInfo.getBaseType().getDesc();
    }

    @Override
    public boolean storageOrcInfo(String dataDir, String dbName, String tableName, String partName, String orcName, boolean stat) {
        OrcInfo orcinfo = ORCUtil.INSTANCE.getOrcinfo(dataDir, dbName, tableName, partName, orcName);
        String lineContent;
        ManifestInfo manifestInfo = null;
        String manifestString = readFile(dataDir + Constant.SLASH + dbName + Constant.SLASH + tableName + Constant.SLASH + partName + Constant.SLASH + Constant.ORC_META_FILE_NAME);
        if (StringUtils.isEmpty(manifestString)) {
            logger.debug("[{}]-[{}]-[{}]第一次写manifest信息", dbName, tableName, partName);
            manifestInfo = new ManifestInfo();
            manifestInfo.addOrcInfo(orcinfo);
            manifestInfo.setDataCount(orcinfo.getRows());
        } else {
            //从原有的manifest信息中获取manifestInfo信息
            manifestInfo = JSON.parseObject(manifestString, ManifestInfo.class);
            manifestInfo.addOrcInfo(orcinfo);
            manifestInfo.setDataCount(manifestInfo.getDataCount() + orcinfo.getRows());
        }
        manifestString = JSON.toJSONString(manifestInfo);
        BufferedWriter bufferedWriter = null;
        try {
            bufferedWriter = new BufferedWriter(new FileWriter(dataDir + Constant.SLASH + dbName + Constant.SLASH + tableName + Constant.SLASH + partName + Constant.SLASH + Constant.ORC_META_FILE_NAME));
            bufferedWriter.write(manifestString);
            bufferedWriter.flush();
        } catch (IOException e) {
            return false;
        } finally {
            if (bufferedWriter != null) {
                try {
                    bufferedWriter.close();
                } catch (IOException e) {
                    logger.error("关闭流失败", e);
                }
            }
        }
        return true;
    }

    @Override
    public boolean storageOrcInfo(OrcInfo orcInfo, boolean stat) {
        return false;
    }

    @Override
    public Set<String> getCurrentOrcFiles() {
        Set<String> result = Sets.newHashSet();
        return result;
    }

    @Override
    public boolean deleteTableManifest(String dbName, String tableName) {
        return false;
    }

    @Override
    public boolean changeOrcManifestStat(List<OrcInfo> falseL, List<OrcInfo> trueL) {
        return false;
    }

    @Override
    public void close() {

    }

    @Override
    public void reloadCache() {

    }

    @Override
    public String getUDCTConfJson() {
        return null;
    }

    @Override
    public Configuration getOrcConfiguration() {
        return null;
    }


    /**
     * 将json文件转换成IndexInfo对象
     *
     * @param jsonFile son文件路径
     * @return 索引结构
     */
    private IndexInfo parseJson2IndexInfo(String jsonFile) {
        String s = readFile(jsonFile);
        IndexInfo indexInfo = JSON.parseObject(s, IndexInfo.class);
        return indexInfo;
    }
}
