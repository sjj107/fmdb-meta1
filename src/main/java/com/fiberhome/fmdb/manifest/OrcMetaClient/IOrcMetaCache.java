package com.fiberhome.fmdb.manifest.OrcMetaClient;

import com.fiberhome.fmdb.manifest.bean.DataRange;
import com.fiberhome.fmdb.manifest.bean.ManifestInfo;
import com.fiberhome.fmdb.manifest.bean.TableManifestInfo;
import com.fiberhome.fmdb.manifest.monitor.ICallback;

import java.util.List;
import java.util.Map;

/**
 * Created by sjj on 19/10/18
 */
public interface IOrcMetaCache {
//    /**
//     * 查询条件获取特定orc文件的路径
//     *
//     * @param dbName    库名
//     * @param tableName 表名
//     * @param colName   字段名
//     * @param start     最小值，为null时，表示不考虑最小值
//     * @param end       最大值，为null时，表示不考虑最大值
//     * @return 所有符合条件的orc文件路径
//     */
//    List<String> getOrcFile(String dbName, String tableName, String colName, String start, String end);

//    /**
//     * 根据库名、表名、字段名获取orc文件名与字段范围的映射信息
//     *
//     * @param dbName    库名
//     * @param tableName 表名
//     * @param colName   字段名
//     * @return <orc文件名，最大值和最小值>
//     */
//    Map<String, DataRange> getColumnRange(String dbName, String tableName, String colName);

    /**
     * 获取表的orc统计信息
     *
     * @param dbName    库名
     * @param tableName 表名
     * @return
     */
    TableManifestInfo getManifestInfo(String dbName, String tableName);

    /**
     * 重新加载信息
     */
    void reloadAllOrcCache();

//    /**
//     * 获取表的全量数据
//     *
//     * @param dbName    库名
//     * @param tableName 表名
//     * @return
//     */
//    long getTableCount(String dbName, String tableName);

    /**
     * 注册回调函数，监控orc元数据的变化，以获取最新的信息
     *
     * @param callback 回调函数
     * @param interval 时间间隔，多久监控一次orc元数据目录，单位毫秒
     */
    void registMonitor(ICallback callback, int interval);
}
