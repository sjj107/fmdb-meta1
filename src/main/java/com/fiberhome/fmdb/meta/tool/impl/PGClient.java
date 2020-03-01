package com.fiberhome.fmdb.meta.tool.impl;

import com.alibaba.druid.pool.DruidDataSourceFactory;
import com.fiberhome.fmdb.common.CommonUtil;
import com.fiberhome.fmdb.common.Constant;
import com.fiberhome.fmdb.common.SQLConstant;
import com.fiberhome.fmdb.manifest.bean.OrcColInfo;
import com.fiberhome.fmdb.manifest.bean.OrcInfo;
import com.fiberhome.fmdb.meta.bean.*;
import com.fiberhome.fmdb.statistic.ORCUtil;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * @Description 访问pg库
 * @Author sjj
 * @Date 19/11/06 下午 02:29
 **/
public class PGClient {
    /**
     * 日志
     */
    private static Logger logger = LoggerFactory.getLogger(PGClient.class);
    //pg的ip信息
    private String pg_ip;
    //pg的端口号信息
    private int pg_port;
    private String pg_user;
    private String pg_psw;

    //pg数据存放位置
    private String pg_dataBase;

    //连接池
    private DataSource dataSource;

    private volatile static PGClient instance;

    public static PGClient getInstance(String pg_ip, int pg_port, String pg_user, String pg_psw, String pg_dataBase) {
        if (instance == null) {
            synchronized (PGClient.class) {
                if (instance == null) {
                    instance = new PGClient(pg_ip, pg_port, pg_user, pg_psw, pg_dataBase);
                }
            }
        }
        return instance;
    }

    private PGClient(String pg_ip, int pg_port, String pg_user, String pg_psw, String pg_dataBase) {
        this.pg_ip = pg_ip;
        this.pg_port = pg_port;
        this.pg_user = pg_user;
        this.pg_psw = pg_psw;
        this.pg_dataBase = pg_dataBase;
        init();
    }

    /**
     * 初始化操作
     */
    private void init() {
        this.dataSource = CommonUtil.INSTANCE.getDataSource(Constant.PG_DRIVER_CLASS, Constant.PG_JDBC_URL_PRE + pg_ip + Constant.COLON + pg_port + pg_dataBase, pg_user, pg_psw);
//        connection = CommonUtil.INSTANCE.getPGConnection(pg_ip, pg_port, pg_dataBase, pg_user, pg_psw);
    }

    public boolean creatUDCT(UDCTInfo udctInfo) {
        Connection connection = null;
        PreparedStatement pstmt = null;
        try {
            connection = dataSource.getConnection();
            connection.setAutoCommit(false);
            pstmt = connection.prepareStatement(SQLConstant.INSERT_UDCT_SQL);
            pstmt.setString(1, udctInfo.getUdct_name().toLowerCase());
            pstmt.setString(2, udctInfo.getWriter());
            pstmt.setString(3, udctInfo.getRead());
            pstmt.setString(4, udctInfo.getBase_type().getDesc().toLowerCase());
            pstmt.setBoolean(5, true);
            pstmt.addBatch();
            pstmt.executeBatch();
            connection.commit();
        } catch (SQLException e) {
            logger.error("创建自定义数据类型[{}]失败", udctInfo, e);
            try {
                connection.rollback();
            } catch (SQLException ex) {
                logger.error("回滚自定义数据类型[{}]失败", udctInfo, ex);
            }
            logger.error("回滚自定义数据类型[{}]成功", udctInfo);
            return false;
        } finally {
            closeSource(null, pstmt, connection);
        }
        logger.info("创建自定义数据类型[{}]成功", udctInfo);
        return true;
    }

    public boolean deleteUDCT(String udctName) {
        Connection connection = null;
        PreparedStatement pstmt = null;
        try {
            connection = dataSource.getConnection();
            connection.setAutoCommit(false);
            pstmt = connection.prepareStatement(SQLConstant.DELETE_UDCT_SQL);
            pstmt.setString(1, udctName.toLowerCase());
            pstmt.addBatch();
            pstmt.executeBatch();
            connection.commit();
        } catch (SQLException e) {
            logger.error("删除数据类型[{}]失败", udctName, e);
            try {
                connection.rollback();
            } catch (SQLException ex) {
                logger.error("回滚删除数据类型[{}]失败", udctName, ex);
            }
            return false;
        } finally {
            closeSource(null, pstmt, connection);
        }
        return true;
    }

    /**
     * 获取所有的自定义数据类型
     *
     * @return
     */
    public List<UDCTInfo> getAllUDCTInfo() {
        List<UDCTInfo> result = Lists.newArrayList();
        Connection connection = null;
        PreparedStatement pstmt = null;
        ResultSet resultSet = null;
        try {
            connection = dataSource.getConnection();
            connection.setAutoCommit(false);
            pstmt = connection.prepareStatement(SQLConstant.SELECT_ALL_UDCT_SQL);
            resultSet = pstmt.executeQuery();
            connection.commit();
            UDCTInfo tmp = null;
            while (resultSet.next()) {
                tmp = new UDCTInfo(resultSet.getString(1), resultSet.getString(2),
                        resultSet.getString(3), FmdbDataType.valueOf(resultSet.getString(4).toUpperCase()));
                result.add(tmp);
            }
        } catch (SQLException e) {
            logger.error("获取自定义类型失败", e);
            try {
                connection.rollback();
            } catch (SQLException ex) {
                logger.error("回滚获取自定义类型失败", ex);
            }
            return null;
        } finally {
            closeSource(resultSet, pstmt, connection);
        }
        return result;
    }

    /**
     * 获取指定的自定义数据类型
     *
     * @param udctName
     * @return
     */
    public UDCTInfo getUDCTInfo(String udctName) {
        UDCTInfo result = null;
        Connection connection = null;
        PreparedStatement pstmt = null;
        ResultSet resultSet = null;
        try {
            connection = dataSource.getConnection();
            connection.setAutoCommit(false);
            pstmt = connection.prepareStatement(SQLConstant.SELECT_UDCT_SQL);
            pstmt.setString(1, udctName);
            resultSet = pstmt.executeQuery();
            connection.commit();
            while (resultSet.next()) {
                String ct_name = resultSet.getString(1);
                String writer = resultSet.getString(2);
                String reader = resultSet.getString(3);
                String base_type = resultSet.getString(4);
                result = new UDCTInfo(ct_name, writer, reader, FmdbDataType.valueOf(base_type.toUpperCase()));
            }
        } catch (SQLException e) {
            logger.error("获取[{}]自定义类型失败", udctName, e);
            try {
                connection.rollback();
            } catch (SQLException ex) {
                logger.error("回滚获取[{}]自定义类型失败", udctName, ex);
            }
            return null;
        } finally {
            closeSource(resultSet, pstmt, connection);
        }
        return result;
    }

    public boolean creatDB(String dbName) {
        Connection connection = null;
        PreparedStatement pstmt = null;
        try {
            connection = dataSource.getConnection();
            connection.setAutoCommit(false);
            pstmt = connection.prepareStatement(SQLConstant.INSERT_DB_SQL);
            pstmt.setString(1, CommonUtil.INSTANCE.genID());
            pstmt.setString(2, dbName);
            pstmt.addBatch();
            pstmt.executeBatch();
            connection.commit();
        } catch (SQLException e) {
            logger.error("创建数据库[{}]失败", dbName, e);
            try {
                connection.rollback();
            } catch (SQLException ex) {
                logger.error("回滚创建数据库[{}]失败", dbName, ex);
            }
            return false;
        } finally {
            closeSource(null, pstmt, connection);
        }
        logger.info("创建数据库[{}]成功", dbName);
        return true;
    }

    public boolean deleteDB(String dbName) {
        Connection connection = null;
        PreparedStatement pstmt = null;
        ResultSet resultSet = null;
        try {
            connection = dataSource.getConnection();
            connection.setAutoCommit(false);
            //获取所有的表
            pstmt = connection.prepareStatement(SQLConstant.SELECT_ALLTABLES_SQL);
            pstmt.setString(1, dbName);
            resultSet = pstmt.executeQuery();
            while (resultSet.next()) {
                deleteTable(dbName, resultSet.getString(1));
            }
            pstmt = connection.prepareStatement(SQLConstant.DELETE_DB_SQL);
            pstmt.setString(1, dbName);
            pstmt.addBatch();
            pstmt.executeBatch();
            connection.commit();
        } catch (SQLException e) {
            logger.error("删除数据库[{}]失败", dbName, e);
            try {
                connection.rollback();
            } catch (SQLException ex) {
                logger.error("回滚删除数据库[{}]失败", dbName, ex);
            }
            return false;
        } finally {
            closeSource(resultSet, pstmt, connection);
        }
        logger.info("删除数据库[{}]成功", dbName);
        return true;
    }

    public boolean storageTable(TableInfo tableInfo) {
        String tableID = CommonUtil.INSTANCE.genID();
        String colID;
        String dbName = tableInfo.getDbName();
        String tableName = tableInfo.getTableName();
        List<ColumnInfo> cols = tableInfo.getCols();
        PartitionInfo partition = tableInfo.getPartition();
        Map<String, String> properties = tableInfo.getProperties();
        List<String> primaryKeys = tableInfo.getPrimaryKeys();
        List<String> sortFields = tableInfo.getSortFields();
        String sortType = tableInfo.getSortType();
        CompressionType compressionType = tableInfo.getCompressionType();
        int orcSize = tableInfo.getOrcSize();
        int ttl = tableInfo.getTtl();
        Connection connection = null;
        PreparedStatement pstmt = null;
        try {
            connection = dataSource.getConnection();
            connection.setAutoCommit(false);
            //插入TBLS表
            pstmt = connection.prepareStatement(SQLConstant.INSERT_TBL_SQL);
            pstmt.setString(1, tableID);
            pstmt.setString(2, dbName);
            pstmt.setString(3, tableName);
            pstmt.setString(4, "table");
            pstmt.setString(5, Joiner.on(",").join(primaryKeys));
            pstmt.setString(6, Joiner.on(",").join(sortFields));
            pstmt.setString(7, sortType);
            pstmt.setString(8, compressionType.toString());
            pstmt.setInt(9, orcSize);
            pstmt.addBatch();
            pstmt.executeBatch();
            //插入COLUMNS表
            pstmt = connection.prepareStatement(SQLConstant.INSERT_COL_SQL);
            String baseType;//实际的字段类型
            for (ColumnInfo col : cols) {
                colID = CommonUtil.INSTANCE.genID();
                String colType = col.getColType().toString();
//                baseType = getBaseType(col.getColType().toLowerCase());
//                if (baseType == null) {
//                    logger.error("[{}]-[{}]-[{}]基础类型为null", dbName, tableName, col.getColName());
//                    return false;
//                }
//                FmdbDataType colType = col.getColType();
//                //如果是自定义类型，需要通过属性获取实际的字段类型
//                if (colType == FmdbDataType.UDCT) {
//                    aColType = col.getProperties().get(Constant.UDCT_KEY);
//                    if (aColType == null) {
//                        logger.error("自定义数据类型，需要通过字段属性设置实际的字段类型");
//                        return false;
//                    }
//                } else {
//                    aColType = FmdbDataType.UDCT.getDesc();
//                }
                pstmt.setString(1, colID);
                pstmt.setString(2, col.getComment());
                pstmt.setString(3, col.getColName());
                pstmt.setString(4, col.getColType().toString());
                pstmt.setInt(5, col.getColIndex());
                pstmt.setBoolean(6, col.isNull());
                pstmt.setInt(7, col.getPrecision());
                pstmt.setInt(8, col.getScale());
                pstmt.setString(9, tableID);
                pstmt.setString(10, "storage");
                pstmt.setString(11, "basetype");
                pstmt.addBatch();
            }
            pstmt.executeBatch();
            //插入PARTITIONS表
            if (partition != null) {
                String part_id = CommonUtil.INSTANCE.genID();
                String col = partition.getCol();
                pstmt = connection.prepareStatement(SQLConstant.INSERT_PARTITIONS_SQL);
                pstmt.setString(1, part_id);
                pstmt.setString(2, dbName);
                pstmt.setString(3, tableName);
                pstmt.setString(4, col);
                pstmt.setString(5, partition.getPartitionType().toString());
                pstmt.setInt(6, ttl);
                pstmt.addBatch();
                pstmt.executeBatch();
                Map<String, String> partPro = partition.getProperties();
                for (String key : partPro.keySet()) {
                    pstmt = connection.prepareStatement(SQLConstant.INSERT_PARTITIONS_PARAMS_SQL);
                    pstmt.setString(1, part_id);
                    pstmt.setString(2, key);
                    pstmt.setString(3, partPro.get(key));
                    pstmt.addBatch();
                    pstmt.executeBatch();
                }
            }

            Set<String> params = properties.keySet();
            for (String param : params) {
                //插入TABLE_PARAMS表
                pstmt = connection.prepareStatement(SQLConstant.INSERT_TBL_PARAM_SQL);
                pstmt.setString(1, tableID);
                pstmt.setString(2, param);
                pstmt.setString(3, properties.get(param));
                pstmt.addBatch();
                pstmt.executeBatch();
            }
//            pstmt = connection.prepareStatement(Constant.INSERT_TBL_PARAM_SQL);
//            pstmt.setString(1, tableID);
//            pstmt.setString(2, "primaryKeys");
//            pstmt.setString(3, Joiner.on(",").join(primaryKeys));
//            pstmt.addBatch();
//            pstmt.executeBatch();
            connection.commit();
        } catch (SQLException e) {
            logger.error("存储表[{}]-[{}]失败", dbName, tableName, e);
            try {
                connection.rollback();
                logger.debug("回滚成功");
            } catch (SQLException ex) {
                logger.error("回滚失败", ex);
            }
            return false;
        } finally {
            closeSource(null, pstmt, connection);
        }
        logger.info("存储表[{}]-[{}]成功", dbName, tableName);
        return true;
    }

    public boolean storageIndex(IndexInfo indexInfo) {
        String indexID = CommonUtil.INSTANCE.genID();
        String colID;
        String indexName = indexInfo.getIndexName();
        String dbName = indexInfo.getDbName();
        String orgTableName = indexInfo.getOrgTableName();
        //索引字段
        List<String> indexCols = indexInfo.getCols();
        //返回字段
        List<String> includes = indexInfo.getIncludes();
        //索引属性
        Map<String, String> properties = indexInfo.getProperties();
        Connection connection = null;
        PreparedStatement pstmt = null;
        try {
            connection = dataSource.getConnection();
            connection.setAutoCommit(false);
            //插入IDXS表
            pstmt = connection.prepareStatement(SQLConstant.INSERT_IDXS_SQL);
            pstmt.setString(1, indexID);
            pstmt.setString(2, dbName);
            pstmt.setString(3, indexName);
            pstmt.setString(4, dbName);
            pstmt.setString(5, orgTableName);
            pstmt.addBatch();
            pstmt.executeBatch();
            //插入TBLS表
            pstmt = connection.prepareStatement(SQLConstant.INSERT_TBL_SQL);
            pstmt.setString(1, indexID);
            pstmt.setString(2, dbName);
            pstmt.setString(3, indexName);
            pstmt.setString(4, "index");
            pstmt.setString(5, "");
            pstmt.addBatch();
            pstmt.executeBatch();
            //插入COLUMNS

            int index = 0;
            ColumnInfo oriColumnInfo = null;
            for (String col : indexCols) {
                colID = CommonUtil.INSTANCE.genID();
                oriColumnInfo = getTableInfo(dbName, orgTableName).getColumnInfo(col);
                if (oriColumnInfo == null) {
                    logger.error("索引[{}]-[{}]的原始表[{}]字段[{}]不存在", dbName, indexName, orgTableName, col);
                    return false;
                }
                pstmt = connection.prepareStatement(SQLConstant.INSERT_COL_SQL);
                pstmt.setString(1, colID);
                pstmt.setString(2, oriColumnInfo.getComment());
                pstmt.setString(3, oriColumnInfo.getColName());
                pstmt.setString(4, oriColumnInfo.getColType().toString());
                pstmt.setInt(5, index);
                pstmt.setBoolean(6, oriColumnInfo.isNull());
                pstmt.setInt(7, oriColumnInfo.getPrecision());
                pstmt.setInt(8, oriColumnInfo.getScale());
                pstmt.setString(9, indexID);
                pstmt.setString(10, "index");
                pstmt.addBatch();
                pstmt.executeBatch();
                index++;
            }
            for (String col : includes) {
                colID = CommonUtil.INSTANCE.genID();
                oriColumnInfo = getTableInfo(dbName, orgTableName).getColumnInfo(col);
                if (oriColumnInfo == null) {
                    logger.error("索引[{}]-[{}]的原始表[{}]不存在", dbName, indexName, orgTableName);
                    return false;
                }
                pstmt = connection.prepareStatement(SQLConstant.INSERT_COL_SQL);
                pstmt.setString(1, colID);
                pstmt.setString(2, oriColumnInfo.getComment());
                pstmt.setString(3, oriColumnInfo.getColName());
                pstmt.setString(4, oriColumnInfo.getColType().toString());
                pstmt.setInt(5, index);
                pstmt.setBoolean(6, oriColumnInfo.isNull());
                pstmt.setInt(7, oriColumnInfo.getPrecision());
                pstmt.setInt(8, oriColumnInfo.getScale());
                pstmt.setString(9, indexID);
                pstmt.setString(10, "include");
                pstmt.addBatch();
                pstmt.executeBatch();
                index++;
            }
            if (properties != null) {
                Set<String> params = properties.keySet();
                for (String param : params) {
                    //插入INDEX_PARAMS表
                    pstmt = connection.prepareStatement(SQLConstant.INSERT_IDX_PARAM_SQL);
                    pstmt.setString(1, indexID);
                    pstmt.setString(2, param);
                    pstmt.setString(3, properties.get(param));
                    pstmt.addBatch();
                    pstmt.executeBatch();
                }
            }
            connection.commit();
        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException ex) {
                logger.error("回滚失败");
            }
            logger.error("存储索引[{}]-[{}]失败", dbName, indexName, e);
            return false;
        } finally {
            closeSource(null, pstmt, connection);
        }
        logger.info("存储索引[{}]-[{}]成功", dbName, indexName);
        return true;
    }


    public boolean deleteTable(String dbName, String tableName) {
        Connection connection = null;
        PreparedStatement pstmt = null;
        ResultSet resultSet = null;
        try {
            connection = dataSource.getConnection();
            connection.setAutoCommit(false);
            //删除该表的所有索引
            pstmt = connection.prepareStatement(SQLConstant.SELECT_ALLINDEXS_SQL);
            pstmt.setString(1, dbName);
            pstmt.setString(2, tableName);
            resultSet = pstmt.executeQuery();
            while (resultSet.next()) {
                deleteIndex(dbName, tableName, resultSet.getString(1));
            }
            //删除分区属性信息
            pstmt = connection.prepareStatement(SQLConstant.DELETE_PARTITION_PARAMS_SQL);
            pstmt.setString(1, dbName);
            pstmt.setString(2, tableName);
            pstmt.addBatch();
            pstmt.executeBatch();
            //删除分区信息
            pstmt = connection.prepareStatement(SQLConstant.DELETE_PARTITIONS_SQL);
            pstmt.setString(1, dbName);
            pstmt.setString(2, tableName);
            pstmt.addBatch();
            pstmt.executeBatch();
            //删除字段信息
            pstmt = connection.prepareStatement(SQLConstant.DELETE_COLUMNS_SQL);
            pstmt.setString(1, dbName);
            pstmt.setString(2, tableName);
            pstmt.addBatch();
            pstmt.executeBatch();
            //删除表属性信息
            pstmt = connection.prepareStatement(SQLConstant.DELETE_TBL_PARAM_SQL);
            pstmt.setString(1, dbName);
            pstmt.setString(2, tableName);
            pstmt.addBatch();
            pstmt.executeBatch();
            //删除表信息
            pstmt = connection.prepareStatement(SQLConstant.DELETE_TBL_SQL);
            pstmt.setString(1, dbName);
            pstmt.setString(2, tableName);
            pstmt.addBatch();
            pstmt.executeBatch();
            connection.commit();
        } catch (SQLException e) {
            logger.error("删除表[{}]-[{}]失败", dbName, tableName, e);
            try {
                connection.rollback();
                logger.debug("回滚成功");
            } catch (SQLException ex) {
                logger.error("回滚失败", ex);
            }
            return false;
        } finally {
            closeSource(resultSet, pstmt, connection);
        }
        deleteTableManifest(dbName, tableName);
        logger.info("删除表[{}]-[{}]成功", dbName, tableName);
        return true;
    }

    public boolean truncateTable(String dbName, String tableName) {
        return deleteTableManifest(dbName, tableName);
    }

    public boolean deleteIndex(String dbName, String tableName, String indexName) {
        Connection connection = null;
        PreparedStatement pstmt = null;
        try {
            connection = dataSource.getConnection();
            connection.setAutoCommit(false);
            pstmt = connection.prepareStatement(SQLConstant.DELETE_COLUMNS_SQL);
            pstmt.setString(1, dbName);
            pstmt.setString(2, indexName);
            pstmt.addBatch();
            pstmt.executeBatch();
            pstmt = connection.prepareStatement(SQLConstant.DELETE_IDX_PARAM_SQL);
            pstmt.setString(1, dbName);
            pstmt.setString(2, indexName);
            pstmt.addBatch();
            pstmt.executeBatch();
            pstmt = connection.prepareStatement(SQLConstant.DELETE_TBL_SQL);
            pstmt.setString(1, dbName);
            pstmt.setString(2, indexName);
            pstmt.addBatch();
            pstmt.executeBatch();
            pstmt = connection.prepareStatement(SQLConstant.DELETE_IDX_SQL);
            pstmt.setString(1, dbName);
            pstmt.setString(2, indexName);
            pstmt.addBatch();
            pstmt.executeBatch();
            connection.commit();
        } catch (SQLException e) {
            logger.error("删除索引[{}]-[{}]失败", dbName, indexName, e);
            try {
                connection.rollback();
            } catch (SQLException ex) {
                logger.error("回滚失败", ex);
            }
            return false;
        } finally {
            closeSource(null, pstmt, connection);
        }
        logger.info("删除索引[{}]-[{}]成功", dbName, indexName);
        deleteTableManifest(dbName, indexName);
        return true;
    }

    public List<String> getAllDBNames() {
        List<String> result = Lists.newArrayList();
        Connection connection = null;
        PreparedStatement pstmt = null;
        ResultSet resultSet = null;
        try {
            connection = dataSource.getConnection();
            connection.setAutoCommit(false);
            pstmt = connection.prepareStatement(SQLConstant.LIST_DB_SQL);
            resultSet = pstmt.executeQuery();
            connection.commit();
            while (resultSet.next()) {
                result.add(resultSet.getString(1));
            }
        } catch (SQLException e) {
            logger.error("查询[{}]失败", SQLConstant.LIST_DB_SQL, e);
        } finally {
            closeSource(resultSet, pstmt, connection);
        }
        return result;
    }

    public List<String> getAllTableName(String dbName) {
        List<String> result = Lists.newArrayList();
        Connection connection = null;
        PreparedStatement pstmt = null;
        ResultSet resultSet = null;
        try {
            connection = dataSource.getConnection();
            connection.setAutoCommit(false);
            pstmt = connection.prepareStatement(SQLConstant.SELECT_ALLTABLES_SQL);
            pstmt.setString(1, dbName);
            resultSet = pstmt.executeQuery();
            while (resultSet.next()) {
                result.add(resultSet.getString(1));
            }
        } catch (SQLException e) {
            logger.error("获取库[{}]下所有的表失败", dbName, e);
        } finally {
            closeSource(resultSet, pstmt, connection);
        }
        return result;
    }

    public List<String> getAllIndexName(String dbName, String tableName) {
        List<String> list = Lists.newArrayList();
        Connection connection = null;
        PreparedStatement pstmt = null;
        ResultSet resultSet = null;
        try {
            connection = dataSource.getConnection();
            connection.setAutoCommit(false);
            pstmt = connection.prepareStatement(SQLConstant.SELECT_ALLINDEXS_SQL);
            pstmt.setString(1, dbName);
            pstmt.setString(2, tableName);
            resultSet = pstmt.executeQuery();
            while (resultSet.next()) {
                list.add(resultSet.getString(1));
            }
        } catch (SQLException e) {
            logger.error("获取表[{}]-[{}]下所有的索引失败", dbName, tableName, e);
            return null;
        } finally {
            closeSource(resultSet, pstmt, connection);
        }
        return list;
    }

    public TableInfo getTableInfo(String dbName, String tableName) {
        TableInfo result = new TableInfo();
        //字段信息
        List<ColumnInfo> columnInfos = Lists.newArrayList();
        //主键信息
        List<String> primaryKeys = Lists.newArrayList();
        //表属性信息
        Map<String, String> properties = Maps.newHashMap();
//        Map<String, String> colProperties = Maps.newHashMap();
        ColumnInfo tmpCol = null;
        Connection connection = null;
        PreparedStatement pstmt = null;
        ResultSet resultSet = null;
        try {
            connection = dataSource.getConnection();
            connection.setAutoCommit(false);
            //字段信息
            pstmt = connection.prepareStatement(SQLConstant.SELECT_COLUMNS_SQL);
            pstmt.setString(1, dbName);
            pstmt.setString(2, tableName);
            resultSet = pstmt.executeQuery();
            while (resultSet.next()) {
                tmpCol = new ColumnInfo();
                tmpCol.setComment(resultSet.getString(1));
                tmpCol.setColName(resultSet.getString(2));
//                String baseType = getBaseType(resultSet.getString(3));
//                if (baseType == null) {
//                    logger.error("[{}]获取基础类型失败", resultSet.getString(3));
//                    return null;
//                }
//                colProperties.put(Constant.UDCT_KEY, resultSet.getString(3));
//                tmpCol.setProperties(colProperties);
                tmpCol.setColType(resultSet.getString(3));
                tmpCol.setBaseType(FmdbDataType.valueOf("string".toUpperCase()));
                tmpCol.setColIndex(resultSet.getInt(4));
                tmpCol.setNull(resultSet.getBoolean(5));
                tmpCol.setPrecision(resultSet.getInt(6));
                tmpCol.setScale(resultSet.getInt(7));
                columnInfos.add(tmpCol);
            }
            if (tmpCol == null) {
                logger.debug("不存在表[{}]-[{}]", dbName, tableName);
                return null;
            }
            //主键信息
            pstmt = connection.prepareStatement(SQLConstant.SELECT_TBL_SQL);
            pstmt.setString(1, dbName);
            pstmt.setString(2, tableName);
            resultSet = pstmt.executeQuery();
            while (resultSet.next()) {
                String primaryksys = resultSet.getString(1);
                String sortFields = resultSet.getString(2);
                String sortType = resultSet.getString(3);
                String compresstype = resultSet.getString(4);
                int orcsize = resultSet.getInt(5);
                if (!StringUtils.isEmpty(primaryksys)) {
                    primaryKeys.addAll(Splitter.on(",").splitToList(primaryksys));
                }
                if (!StringUtils.isEmpty(sortFields)) {
                    properties.put(Constant.SORT_FIELDS, sortFields);
                }
                if (!StringUtils.isEmpty(sortType)) {
                    properties.put(Constant.SORT_TYPE, sortType);
                }
                if (!StringUtils.isEmpty(compresstype)) {
                    properties.put(Constant.COMPRESSTYPE, compresstype);
                }
                properties.put(Constant.ORC_SIZE, String.format("%d", orcsize));
            }
            //分区信息
            pstmt = connection.prepareStatement(SQLConstant.SELECT_PARTITIONS_SQL);
            pstmt.setString(1, dbName);
            pstmt.setString(2, tableName);
            resultSet = pstmt.executeQuery();
            String partId = null;
            String colName = null;
            String colID = null;
            String partType = null;
            String ttl = null;
            while (resultSet.next()) {
                partId = resultSet.getString(1);
                colID = resultSet.getString(2);
                partType = resultSet.getString(3);
                ttl = resultSet.getString(4);
            }
            if (partId != null) {
                pstmt = connection.prepareStatement(SQLConstant.SELECT_COLUMN_NAME_SQL);
                pstmt.setString(1, colID);
                resultSet = pstmt.executeQuery();
                while (resultSet.next()) {
                    colName = resultSet.getString(1);
                }
                properties.put(Constant.PARTITION_NAME, colName);
                properties.put(Constant.PARTITION_TYPE, partType);
                properties.put(Constant.TTL, ttl);
//                PartitionInfo partitionInfo = new PartitionInfo(dbName, tableName, colName, PartitionType.valueOf(partType));
//                pstmt = connection.prepareStatement(SQLConstant.SELECT_PARTITION_PARAMS_SQL);
//                pstmt.setString(1, partId);
//                resultSet = pstmt.executeQuery();
//                Map<String, String> partPro = Maps.newHashMap();
//                while (resultSet.next()) {
//                    partitionInfo.addProperties(resultSet.getString(1), resultSet.getString(2));
//                }
//                result.setPartition(partitionInfo);
            }


            //属性信息
            pstmt = connection.prepareStatement(SQLConstant.SELECT_TBL_PARAMS_SQL);
            pstmt.setString(1, dbName);
            pstmt.setString(2, tableName);
            resultSet = pstmt.executeQuery();
            while (resultSet.next()) {
                properties.put(resultSet.getString(1), resultSet.getString(2));
            }
            result.setDbName(dbName);
            result.setTableName(tableName);
            result.setCols(columnInfos);
            result.setPrimaryKeys(primaryKeys);
            result.setProperties(properties);
        } catch (SQLException e) {
            logger.error("获取表[{}]-[{}]失败", dbName, tableName, e);
            try {
                connection.rollback();
            } catch (SQLException ex) {
                logger.error("回滚失败");
            }
            return null;
        } catch (Exception e) {
            logger.error("获取表[{}]-[{}]失败", dbName, tableName, e);
            try {
                connection.rollback();
            } catch (SQLException ex) {
                logger.error("回滚失败");
            }
            return null;
        } finally {
            closeSource(resultSet, pstmt, connection);
        }
        List<String> allIndexName = getAllIndexName(dbName, tableName);
        List<IndexInfo> indexs = Lists.newArrayList();
        for (String indexName : allIndexName) {
            indexs.add(getIndexInfo(dbName, indexName));
        }
        result.setIndexs(indexs);
        return result;
    }

    /**
     * 根据实际字段类型获取基础类型
     *
     * @param actureType
     * @return
     */
    private String getBaseType(String actureType) {
        String result = null;
        Connection connection = null;
        PreparedStatement pstmt = null;
        ResultSet resultSet = null;
        try {
            connection = dataSource.getConnection();
            connection.setAutoCommit(false);
            //字段信息
            pstmt = connection.prepareStatement(SQLConstant.SELECT_BASETYPEBYACTYPE_SQL);
            pstmt.setString(1, actureType);
            resultSet = pstmt.executeQuery();
            while (resultSet.next()) {
                result = resultSet.getString(1);
            }

        } catch (Exception e) {
            logger.error("获取字段[{}]的基础类型失败", actureType, e);
            try {
                connection.rollback();
            } catch (SQLException ex) {
                logger.error("回滚失败");
            }
            return null;
        } finally {
            closeSource(resultSet, pstmt, connection);
        }
        return result;
    }

    public IndexInfo getIndexInfo(String dbName, String indexName) {
        IndexInfo result = new IndexInfo();
        String oriTableName = null;
        Map<String, String> properties = Maps.newHashMap();
        List<String> indexCols = Lists.newArrayList();
        List<String> includes = Lists.newArrayList();
        Connection connection = null;
        PreparedStatement pstmt = null;
        ResultSet resultSet = null;
        try {
            connection = dataSource.getConnection();
            connection.setAutoCommit(false);
            //原始表名
            pstmt = connection.prepareStatement(SQLConstant.SELECT_ORITABLENAME_SQL);
            pstmt.setString(1, dbName);
            pstmt.setString(2, indexName);
            resultSet = pstmt.executeQuery();
            while (resultSet.next()) {
                if (!StringUtils.isEmpty(oriTableName)) {
                    logger.error("索引[{}]-[{}]对应多个原始表", dbName, indexName);
                    throw new RuntimeException("索引[" + dbName + "]-[{" + indexName + "}]对应多个原始表");
                }
                oriTableName = resultSet.getString(1);
                if (StringUtils.isEmpty(oriTableName)) {
                    logger.warn("索引[{}]-[{}]没有原始表", dbName, indexName);
                    return null;
                }
            }
            //索引字段
            pstmt = connection.prepareStatement(SQLConstant.SELECT_INDEXCOL_SQL);
            pstmt.setString(1, dbName);
            pstmt.setString(2, indexName);
            resultSet = pstmt.executeQuery();
            while (resultSet.next()) {
                indexCols.add(resultSet.getString(1));
            }
            //include字段
            pstmt = connection.prepareStatement(SQLConstant.SELECT_INCLUDECOL_SQL);
            pstmt.setString(1, dbName);
            pstmt.setString(2, indexName);
            resultSet = pstmt.executeQuery();
            while (resultSet.next()) {
                includes.add(resultSet.getString(1));
            }
            //索引属性
            pstmt = connection.prepareStatement(SQLConstant.SELECT_TBL_PARAMS_SQL);
            pstmt.setString(1, dbName);
            pstmt.setString(2, indexName);
            resultSet = pstmt.executeQuery();
            while (resultSet.next()) {
                properties.put(resultSet.getString(1), resultSet.getString(2));
            }
            connection.commit();
        } catch (SQLException e) {
            logger.error("获取索引[{}]-[{}]信息失败", dbName, indexName, e);
            try {
                connection.rollback();
            } catch (SQLException ex) {
                logger.error("回滚失败");
            }
            return null;
        } finally {
            closeSource(resultSet, pstmt, connection);
        }
        result.setDbName(dbName);
        result.setIndexName(indexName);
        result.setOrgTableName(oriTableName);
        result.setCols(indexCols);
        result.setIncludes(includes);
        result.setProperties(properties);
        return result;
    }

    public boolean  truncateTableCloumnSize(){
        Connection connection = null;
        PreparedStatement pstmt = null;
        ResultSet resultSet = null;

        try {
            connection = dataSource.getConnection();
            connection.setAutoCommit(false);

            Statement statement = connection.createStatement();

            statement.execute(SQLConstant.TRUNCATE_SQL);

            connection.commit();

            return  true;
        }catch (SQLException e){
            e.printStackTrace();
            return false;
        }finally {
            closeSource(resultSet, pstmt, connection);
        }

    }
    public boolean storageCloumnSize(List<StaticColumn> staticColumnS) {
        Connection connection = null;
        PreparedStatement pstmt = null;
        ResultSet resultSet = null;
        try {
            connection = dataSource.getConnection();
            connection.setAutoCommit(false);

            pstmt = connection.prepareStatement(SQLConstant.INSERT_STAS_SQL);

            for (int i = 0; i < staticColumnS.size(); i++) {
                StaticColumn staticColumn = staticColumnS.get(i);

                String dbName = staticColumn.getDbName();
                String tableName = staticColumn.getTableName();
                String colunmName = staticColumn.getColunmName();
                long usedBytes = staticColumn.getUsedBytes();

                pstmt.setString(1, dbName);
                pstmt.setString(2, tableName);
                pstmt.setString(3, colunmName);
                pstmt.setLong(4, usedBytes);

                pstmt.addBatch();
            }
            pstmt.executeBatch();
            connection.commit();
            return true;
        } catch (SQLException e) {

            try {
                connection.rollback();
                logger.debug("回滚成功");
            } catch (SQLException ex) {
                logger.error("回滚失败");
            }
            return false;
        } finally {
            closeSource(resultSet, pstmt, connection);
        }

    }

    public boolean storageOrcInfo(OrcInfo orcinfo, boolean stat) {
//        OrcInfo orcinfo = ORCUtil.INSTANCE.getOrcinfo(dataDir, dbName, tableName, partName, orcName);
//        if (orcinfo == null) {
//            logger.debug("[{}]-[{}]-[{}]-[{}]文件入库未完成", dbName, tableName, partName, orcName);
//            return false;
//        }
        String dbName = orcinfo.getDbName();
        String tableName = orcinfo.getTblName();
        String partName = orcinfo.getPartName();
        String orcName = orcinfo.getName();
        long preCount = -1;
        long rows = orcinfo.getRows();
        Map<String, OrcColInfo> colInfoMap = orcinfo.getColInfoMap();
        Connection connection = null;
        PreparedStatement pstmt = null;
        ResultSet resultSet = null;
        try {
            connection = dataSource.getConnection();
            connection.setAutoCommit(false);
//            pstmt = connection.prepareStatement(SQLConstant.SELECT_TBLCOUNT_SQL);
//            pstmt.setString(1, dbName);
//            pstmt.setString(2, tableName);
//            pstmt.setString(3, partName);
//            resultSet = pstmt.executeQuery();
//            while (resultSet.next()) {
//                preCount = resultSet.getLong(1);
//            }
//
//            if (preCount == -1) {
//                pstmt = connection.prepareStatement(SQLConstant.INSERT_TBLCOUNT_SQL);
//                pstmt.setString(1, dbName);
//                pstmt.setString(2, tableName);
//                pstmt.setString(3, partName);
//                pstmt.setLong(4, rows);
//            } else {
//                pstmt = connection.prepareStatement(SQLConstant.UPDATE_TBLCOUNT_SQL);
//                pstmt.setLong(1, preCount + rows);
//                pstmt.setString(2, dbName);
//                pstmt.setString(3, tableName);
//                pstmt.setString(4, partName);
//            }
//            pstmt.addBatch();
//            pstmt.executeBatch();
            pstmt = connection.prepareStatement(SQLConstant.INSERT_MANIFEST_SQL);
            for (OrcColInfo orcColInfo : colInfoMap.values()) {
                String min = orcColInfo.getMin();
                String max = orcColInfo.getMax();
                long count = orcColInfo.getCount();
                pstmt.setString(1, dbName);
                pstmt.setString(2, tableName);
                pstmt.setString(3, partName);
                pstmt.setString(4, orcName);
                pstmt.setString(5, orcColInfo.getColName());
                pstmt.setString(6, (min == null) ? null : orcColInfo.getMin().replaceAll("\u0000", ""));
                pstmt.setString(7, (max == null) ? null : orcColInfo.getMax().replaceAll("\u0000", ""));
                pstmt.setLong(8, orcColInfo.getCount());
                pstmt.setBoolean(9, (rows == count) ? false : true);
                pstmt.setLong(10, rows - count);
                pstmt.setBoolean(11, stat);
                pstmt.addBatch();
            }
            pstmt.executeBatch();
            connection.commit();
            logger.debug("存储orc[{}]-[{}]-[{}]-[{}]文件元数据成功", dbName, tableName, partName, orcName);
        } catch (SQLException e) {
            logger.error("存储orc[{}]-[{}]-[{}]-[{}]文件元数据失败", dbName, tableName, partName, orcName, e);
            try {
                connection.rollback();
                logger.debug("回滚成功");
            } catch (SQLException ex) {
                logger.error("回滚失败");
            }
            return true;
        } finally {
            closeSource(resultSet, pstmt, connection);
        }
        return true;
    }

    public Set<String> getCurrentOrcFiles() {
        Connection connection = null;
        PreparedStatement pstmt = null;
        ResultSet resultSet = null;
        Set<String> result = Sets.newHashSet();
        try {
            connection = dataSource.getConnection();
            connection.setAutoCommit(false);
            pstmt = connection.prepareStatement(SQLConstant.SELECT_MANIFEST_ORCNAME_SQL);
            resultSet = pstmt.executeQuery();
            while (resultSet.next()) {
                result.add(resultSet.getString(1) + Constant.UNDERLINE + resultSet.getString(2) + Constant.UNDERLINE + resultSet.getString(3));
            }
        } catch (SQLException e) {
            logger.error("查询[{}]出错", SQLConstant.SELECT_MANIFEST_ORCNAME_SQL, e);
        } finally {
            closeSource(resultSet, pstmt, connection);
        }
        return result;
    }

    public boolean deleteTableManifest(String dbName, String tableName) {
        Connection connection = null;
        PreparedStatement pstmt = null;
        try {
            connection = dataSource.getConnection();
            connection.setAutoCommit(false);
            pstmt = connection.prepareStatement(SQLConstant.DELETE_TBL_MANIFEST_SQL);
            pstmt.setString(1, dbName);
            pstmt.setString(2, tableName);
            pstmt.addBatch();
            pstmt.executeBatch();
//            pstmt = connection.prepareStatement(SQLConstant.DELETE_TBLCOUNT_SQL);
//            pstmt.setString(1, dbName);
//            pstmt.setString(2, tableName);
//            pstmt.addBatch();
//            pstmt.executeBatch();
            connection.commit();
            logger.debug("删除表[{}]-[{}]的manifest信息成功", dbName, tableName);
        } catch (SQLException e) {
            logger.error("删除表[{}]-[{}]的manifest信息失败", dbName, tableName, e);
            try {
                connection.rollback();
                logger.debug("回滚成功");
            } catch (SQLException ex) {
                logger.error("回滚失败", ex);
            }
            return false;
        } finally {
            closeSource(null, pstmt, connection);
        }
        return true;
    }

    public void close() {
    }

    public boolean changeOrcManifestStat(List<OrcInfo> falseL, List<OrcInfo> trueL) {
        Connection connection = null;
        PreparedStatement pstmt = null;
        try {
            connection = dataSource.getConnection();
            connection.setAutoCommit(false);
            pstmt = connection.prepareStatement(SQLConstant.UPDATE_ORC_MANIFEST_STAT);
            for (OrcInfo orcInfo : trueL) {
                pstmt.setBoolean(1, true);
                pstmt.setString(2, orcInfo.getDbName());
                pstmt.setString(3, orcInfo.getTblName());
                pstmt.setString(4, orcInfo.getPartName());
                pstmt.setString(5, orcInfo.getName());
                pstmt.addBatch();
            }
            pstmt.executeBatch();
            pstmt = connection.prepareStatement(SQLConstant.DELETE_ORC_MANIFEST);
            for (OrcInfo orcInfo : falseL) {
                pstmt.setString(1, orcInfo.getDbName());
                pstmt.setString(2, orcInfo.getTblName());
                pstmt.setString(3, orcInfo.getPartName());
                pstmt.setString(4, orcInfo.getName());
                pstmt.addBatch();
            }
            pstmt.executeBatch();
            connection.commit();
        } catch (SQLException e) {
            logger.error("修改状态失败", e);
            try {
                connection.rollback();
                logger.error("回滚成功");
            } catch (SQLException ex) {
                logger.error("回滚失败", ex);
            }
            return false;
        } finally {
            closeSource(null, pstmt, connection);
        }
        return true;
    }

    private void closeSource(ResultSet resultSet, PreparedStatement pstmt, Connection connection) {
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
            logger.error("关闭连接失败", e);
        }
    }
}
