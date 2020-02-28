package com.fiberhome.fmdb.meta.bean;

import com.fiberhome.fmdb.common.Constant;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class TableInfo implements Serializable {
    private static final long serialVersionUID = 1463382646064875288L;
    private static Logger logger = LoggerFactory.getLogger("fmdbmeta");
    //表名
    private String tableName;
    //所属DB
    private String dbName;
    //所有的字段信息
    private List<ColumnInfo> cols = Lists.newArrayList();
    //主键信息
    private List<String> primaryKeys = Lists.newArrayList();
    //字段名与字段信息的映射
    private Map<String, ColumnInfo> colNameMap = Maps.newHashMap();
    //字段下标与字段信息的映射
    private Map<Integer, ColumnInfo> colIndexMap = Maps.newHashMap();
    //表属性信息
    private Map<String, String> properties = Maps.newHashMap();
    //所有的索引信息
    private List<IndexInfo> indexs = Lists.newArrayList();
    //分区信息
    private PartitionInfo partition;
    //排序字段列表
    private List<String> sortFields = Lists.newArrayList();
    //排序类型，true为升序，false为降序，默认为升序
    private String sortType = "ASC";
    //ttl
    private int ttl = -1;
    //压缩方式
    private CompressionType compressionType = CompressionType.ZSTD;
    //每个orc文件的大小,默认128（单位：M）
    private int orcSize = 128;

    public TableInfo() {
    }

    public TableInfo(String dbName, String tableName, List<ColumnInfo> cols, List<String> primaryKeys) {
        this.tableName = tableName;
        this.dbName = dbName;
        this.cols = cols;
        for (ColumnInfo col : cols) {
            this.colNameMap.put(col.getColName(), col);
            this.colIndexMap.put(col.getColIndex(), col);
        }
        this.primaryKeys = primaryKeys;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public List<ColumnInfo> getCols() {
        return cols;
    }

    public ColumnInfo getColumnInfo(String colName) {
        return this.colNameMap.get(colName);
    }

    public ColumnInfo getColumnInfo(int colIndex) {
        return this.colIndexMap.get(colIndex);
    }

    public List<String> getPrimaryKeys() {
        return primaryKeys;
    }

    public int getOrcSize() {
        return orcSize;
    }

    public void setPrimaryKeys(List<String> primaryKeys) {
        this.primaryKeys = primaryKeys;
    }

    public List<IndexInfo> getIndexs() {
        return indexs;
    }

    public void setIndexs(List<IndexInfo> indexs) {
        this.indexs = indexs;
    }

    public void setCompressionType(CompressionType compressionType) {
        this.compressionType = compressionType;
    }

    public PartitionInfo getPartition() {
        return partition;
    }

    public List<String> getSortFields() {
        return sortFields;
    }

    public String getSortType() {
        if (this.sortFields.size() == 0) {
            return "";
        }
        return sortType;
    }

    public int getTtl() {
        return ttl;
    }

    public CompressionType getCompressionType() {
        return compressionType;
    }

    public void setCols(List<ColumnInfo> cols) {
        this.cols = cols;
        for (ColumnInfo col : cols) {
            this.colNameMap.put(col.getColName(), col);
            this.colIndexMap.put(col.getColIndex(), col);
        }
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        //分区字段名
        String partName = properties.get(Constant.PARTITION_NAME);
        if (!StringUtils.isEmpty(partName)) {
            if (this.cols.contains(partName)) {
                logger.error("分区字段[{}]不在字段列表中[{}]", partName, this.cols);
                throw new IllegalArgumentException("分区字段不在字段列表中");
            }
            //分区类型
            String partType = properties.get(Constant.PARTITION_TYPE);
            if (StringUtils.isEmpty(partName)) {
                logger.error("表[{}]设置了分区字段，分区类型不能为空", tableName);
                throw new IllegalArgumentException("分区类型不能为空");
            }
            String ttl = properties.get(Constant.TTL);
            if (!StringUtils.isEmpty(ttl)) {
                this.ttl = Integer.parseInt(ttl);
                properties.remove(Constant.TTL);
            } else {
                logger.warn("未设置TTL，使用默认值-1");
                this.ttl = -1;
            }
            this.partition = new PartitionInfo(dbName, tableName, partName, PartitionType.valueOf(partType.toUpperCase()));
            properties.remove(Constant.PARTITION_NAME);
            properties.remove(Constant.PARTITION_TYPE);
        } else {
            if (!StringUtils.isEmpty(properties.get(Constant.PARTITION_TYPE))) {
                logger.error("表[{}]未设置分区字段", tableName);
                throw new IllegalArgumentException("表未设置分区字段");
            }
            if (!StringUtils.isEmpty(properties.get(Constant.TTL))) {
                logger.error("表[{}]未设置分区字段", tableName);
                throw new IllegalArgumentException("表未设置分区字段");
            }
        }
        //排序字段F
        String sortFields = properties.get(Constant.SORT_FIELDS);
        if (!StringUtils.isEmpty(sortFields)) {
            this.sortFields = Lists.newArrayList(sortFields.split(Constant.COMMA));
            properties.remove(Constant.SORT_FIELDS);
        }
        //排序类型
        String sortType = properties.get(Constant.SORT_TYPE);
        if (!StringUtils.isEmpty(sortType)) {
            if (!(sortType.equalsIgnoreCase("ASC") || sortType.equalsIgnoreCase("DESC"))) {
                logger.warn("排序类型设置错误，支持ASC（代表升序）或DESC（代表降序），使用默认值ASC");
            } else {
                this.sortType = sortType;
            }
            properties.remove(Constant.SORT_TYPE);
        }
        //压缩方式
        String compressType = properties.get(Constant.COMPRESSTYPE);
        if (!StringUtils.isEmpty(compressType)) {
            compressionType = CompressionType.valueOf(compressType.toUpperCase());
            properties.remove(Constant.COMPRESSTYPE);
        }
        //orc文件大小
        String orcSizeS = properties.get(Constant.ORC_SIZE);
        if (!StringUtils.isEmpty(orcSizeS)) {
            try {
                orcSize = Integer.parseInt(orcSizeS);
            } catch (NumberFormatException e) {
                logger.error("orc.size设置错误");
            }
            properties.remove(Constant.ORC_SIZE);
        }
        this.properties = properties;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TableInfo tableInfo = (TableInfo) o;
        return tableName.equals(tableInfo.tableName) &&
                dbName.equals(tableInfo.dbName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableName, dbName);
    }
}
