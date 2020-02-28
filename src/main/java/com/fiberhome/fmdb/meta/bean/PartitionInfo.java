package com.fiberhome.fmdb.meta.bean;

import com.google.common.collect.Maps;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * @Description 分区信息
 * @Author sjj
 * @Date 19/11/08 上午 09:53
 **/
public class PartitionInfo implements Serializable {
    private static final long serialVersionUID = -4283893100911885136L;
    /**
     * 分区字段
     */
    private String col; // required
    /**
     * 库名
     */
    private String dbName; // required
    /**
     * 表名
     */
    private String tableName; // required
    /**
     * 分区类型
     */
    private PartitionType partitionType;
    /**
     * 分区属性
     */
    private Map<String, String> properties = Maps.newHashMap();

    public PartitionInfo() {
    }

    public PartitionInfo(String dbName, String tableName, String col, PartitionType partitionType) {
        this.dbName = dbName;
        this.tableName = tableName;
        this.col = col;
        this.partitionType = partitionType;
    }

    public String getCol() {
        return col;
    }

    public void setCol(String col) {
        this.col = col;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public void addProperties(String key, String value) {
        this.properties.put(key, value);
    }

    public void removeProperties(String key) {
        this.properties.remove(key);
    }

    public PartitionType getPartitionType() {
        return partitionType;
    }

    public void setPartitionType(PartitionType partitionType) {
        this.partitionType = partitionType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PartitionInfo that = (PartitionInfo) o;
        return col.equals(that.col) &&
                dbName.equals(that.dbName) &&
                tableName.equals(that.tableName) &&
                partitionType == that.partitionType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(col, dbName, tableName, partitionType);
    }
}
