package com.fiberhome.fmdb.meta.bean;

/**
 * 统计列信息
 */
public class StaticColumn {

    // 库名
    private String dbName;

    // 表名
    private String tableName;

    // 列名
    private  String colunmName;

    // 使用字节数
    private  long usedBytes;

    // 备用字段
    private String remark;

    public StaticColumn(String dbName, String tableName, String colunmName, long usedBytes) {
        this.dbName = dbName;
        this.tableName = tableName;
        this.colunmName = colunmName;
        this.usedBytes = usedBytes;
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

    public String getColunmName() {
        return colunmName;
    }

    public void setColunmName(String colunmName) {
        this.colunmName = colunmName;
    }

    public long getUsedBytes() {
        return usedBytes;
    }

    public void setUsedBytes(long usedBytes) {
        this.usedBytes = usedBytes;
    }

    public String getRemark() {
        return remark;
    }

    public void setRemark(String remark) {
        this.remark = remark;
    }

    @Override
    public String toString() {
        return "StaticColumn{" +
                "dbName='" + dbName + '\'' +
                ", tableName='" + tableName + '\'' +
                ", colunmName='" + colunmName + '\'' +
                ", usedBytes=" + usedBytes +
                ", remark='" + remark + '\'' +
                '}';
    }
}
