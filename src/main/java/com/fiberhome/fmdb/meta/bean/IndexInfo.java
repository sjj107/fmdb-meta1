package com.fiberhome.fmdb.meta.bean;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class IndexInfo implements Serializable {
    private static final long serialVersionUID = -282242074836175215L;
    //索引名
    private String indexName;
    //原始表名
    private String orgTableName;
    //所属数据库
    private String dbName;
    //索引字段
    private List<String> cols;
    //INCLUDE字段
    private List<String> includes;
    //索引属性
    private Map<String, String> properties;

    public IndexInfo() {
    }

    public IndexInfo(String indexName, String orgTableName, String dbName, List<String> cols) {
        this.indexName = indexName;
        this.orgTableName = orgTableName;
        this.dbName = dbName;
        this.cols = cols;
    }

    //    /**
//     * 根据字段名获取字段信息
//     *
//     * @param colName
//     * @return
//     */
//    public ColumnInfo getColumnInfo(String colName) {
//        Factory factory = new LocalFactory();
//        IFMDBMetaClient metaClient = factory.getMetaClient();
//        return metaClient.getTableInfo(dbName, orgTableName).getColumnInfo(colName);
//    }

    public String getIndexName() {
        return indexName;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    public String getOrgTableName() {
        return orgTableName;
    }

    public void setOrgTableName(String orgTableName) {
        this.orgTableName = orgTableName;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public List<String> getCols() {
        return cols;
    }

    public void setCols(List<String> cols) {
        this.cols = cols;
    }

    public List<String> getIncludes() {
        return includes;
    }

    public void setIncludes(List<String> includes) {
        this.includes = includes;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }
}
