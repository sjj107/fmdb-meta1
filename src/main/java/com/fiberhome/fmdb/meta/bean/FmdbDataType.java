package com.fiberhome.fmdb.meta.bean;

/**
 * Created by sjj on 19/10/25
 * 目前ORC支持的类型
 */
public enum FmdbDataType {
    BOOLEAN("boolean", "string"),
    TINYINT("tinyint", "num"),
    SMALLINT("smallint", "num"),
    INT("int", "num"),
    BIGINT("bigint", "num"),
    FLOAT("float", "num"),
    DOUBLE("double", "num"),
    STRING("string", "string"),
    DATE("date", "string"),
    TIMESTAMP("timestamp", "string"),
    BINARY("binary", "string"),
    DECIMAL("decimal", "num"),
    VARCHAR("varchar", "string"),
    CHAR("char", "string");
    private String desc;
    private String sortType;

    FmdbDataType(String desc, String sortType) {
        this.desc = desc;
        this.sortType = sortType;
    }

    public String getDesc() {
        return desc;
    }

    public String getSortType() {
        return sortType;
    }
}
