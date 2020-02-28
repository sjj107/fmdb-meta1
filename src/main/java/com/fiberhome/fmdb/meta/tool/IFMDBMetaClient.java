package com.fiberhome.fmdb.meta.tool;

import com.fiberhome.fmdb.manifest.bean.OrcInfo;
import com.fiberhome.fmdb.meta.bean.Category;
import com.fiberhome.fmdb.meta.bean.IndexInfo;
import com.fiberhome.fmdb.meta.bean.TableInfo;
import com.fiberhome.fmdb.meta.bean.UDCTInfo;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;

import java.util.List;
import java.util.Set;

/**
 * Created by sjj on 19/10/16
 */
public interface IFMDBMetaClient {

    List<Category> getCategories();
    /**
     * 创建自定义数据类型
     *
     * @param udctInfo 自定义数据类型信息
     * @return
     */
    boolean creatUDCT(UDCTInfo udctInfo);

    /**
     * 删除自定义数据类型
     *
     * @param udctName 自定义数据类型名称
     * @return
     */
    boolean deleteUDCT(String udctName);

    /**
     * 获取自定义数据类型信息
     *
     * @param udctName 自定义数据类型名称
     * @return
     */
    UDCTInfo getUDCTInfo(String udctName);

    /**
     * 获取所有的自定义数据类型名称
     *
     * @return
     */
    Set<String> getAllUDCTNames();

    /**
     * 新建库
     *
     * @param dbName 库名
     * @return 是否成功
     */
    boolean creatDB(String dbName);

    /**
     * 删除库
     *
     * @param dbName 库名
     * @return 是否成功
     */
    boolean deleteDB(String dbName);

    /**
     * 存储表结构
     *
     * @param tableInfo 表信息
     * @return 是否存储成功
     */
    boolean storageTable(TableInfo tableInfo);

    /**
     * 存储索引结构
     *
     * @param indexInfo 索引信息
     * @return 是否存储成功
     */
    boolean storageIndex(IndexInfo indexInfo);

    /**
     * 删除表结构
     *
     * @param dbName    库名
     * @param tableName 表名
     * @return 是否删除成功
     */
    boolean deleteTable(String dbName, String tableName);

    /**
     * 清空表数据
     *
     * @param dbName    库名
     * @param tableName 表名
     * @return
     */
    boolean truncateTable(String dbName, String tableName);

    /**
     * 删除索引结构
     *
     * @param dbName    库名
     * @param tableName 表名
     * @param indexName 索引名
     * @return 是否删除成功
     */
    boolean deleteIndex(String dbName, String tableName, String indexName);

    /**
     * 获取所有的库名
     *
     * @return 所有库名
     */
    Set<String> getAllDBNames();

    /**
     * 获取指定库下所有的表名
     *
     * @param dbName 库名
     * @return 所有的表名
     */
    List<String> getAllTableName(String dbName);

    /**
     * 获取指定库和表所有的索引名
     *
     * @param dbName    库名
     * @param tableName 表名
     * @return 所有的索引名
     */
    List<String> getAllIndexName(String dbName, String tableName);

    /**
     * 根据库名和表名获取表结构
     *
     * @param dbName    库名
     * @param tableName 表名
     * @return 表结构
     */
    TableInfo getTableInfo(String dbName, String tableName);

    /**
     * 根据库名和索引名获取索引结构
     *
     * @param dbName    库名
     * @param indexName 索引名
     * @return 索引结构
     */
    IndexInfo getIndexInfo(String dbName, String indexName);

    /**
     * 根据库名，表名，以及字段名，获取字段类型
     *
     * @param dbName    库名
     * @param tableName 表名
     * @param colName   字段名
     * @return
     */
    String getColType(String dbName, String tableName, String colName);

//    /**
//     * 获取索引字段的原始字段信息
//     *
//     * @param dbName    库名
//     * @param indexName 索引名
//     * @param colName   字段名
//     * @return
//     */
//    @Deprecated
//    ColumnInfo getOriColumnInfo(String dbName, String indexName, String colName);

    /**
     * 存储orc文件的信息
     *
     * @param dataDir   数据目录
     * @param dbName    库名
     * @param tableName 表名
     * @param tableName 分区名
     * @param orcName   orc文件名
     * @param stat      orc manifest的状态
     * @return 是否成功
     */
    boolean storageOrcInfo(String dataDir, String dbName, String tableName, String partName, String orcName, boolean stat);

    /**
     * 存储orc元数据信息
     *
     * @param orcInfo
     * @param stat    orc manifest的状态
     * @return
     */
    boolean storageOrcInfo(OrcInfo orcInfo, boolean stat);

    /**
     * 获取当前已经存储了哪些ORC文件
     *
     * @return dbName_tableName_orcName 的集合
     */
    Set<String> getCurrentOrcFiles();

    /**
     * 根据库名和表名（真实表或索引表），删除对应的manifest信息
     *
     * @param dbName    库名
     * @param tableName 表名
     * @return
     */
    boolean deleteTableManifest(String dbName, String tableName);

    /**
     * 修改orc manifest信息状态的接口，事物操作
     *
     * @param falseL 设置为false的orc列表
     * @param trueL  设置为true的orc列表
     * @return
     */
    boolean changeOrcManifestStat(List<OrcInfo> falseL, List<OrcInfo> trueL);

    /**
     * 关闭相关资源
     */
    void close();

    //跟新缓存
    void reloadCache();

    /**
     * 获取自定义数据类型的json串，用于Configuration
     * @return
     */
    String getUDCTConfJson();


    /**
     * 获取唯一的一个 orc configuration   解决orc枚举加载问题
     * 1.get_json
     * 2.get conf
     * 3.registFiled
     *
     *
     * @return
     */
    Configuration  getOrcConfiguration();


}
