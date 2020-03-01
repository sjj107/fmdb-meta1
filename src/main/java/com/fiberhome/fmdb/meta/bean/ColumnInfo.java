package com.fiberhome.fmdb.meta.bean;

import com.fiberhome.fmdb.common.Constant;
import com.google.common.collect.Maps;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;

public class ColumnInfo implements Serializable {

    private static final long serialVersionUID = 104177845062484925L;
    //字段描述
    private String comment;
    //字段名
    private String colName;
    //字段类型
    private FmdbTypeDesc colType;
    //基础类型
    private FmdbDataType baseType;
    //字段顺序
    private int colIndex;

    //是否能为空
    private boolean isNull = true;
    //精度，表示该字段的有效数字位数
    private int precision;
    //刻度、取值范围，表示该字段的小数位数
    private int scale;
//    //colType为UDCT时，需要设置，key为customized_type_name，value为实际的类型，如：mac；
//    private Map<String, String> properties = Maps.newHashMap();

    public ColumnInfo() {
    }

    /**
     * ColumnInfo构造类
     *
     * @param colName  字段名
     * @param colType  字段类型
     * @param colIndex 字段下标
     *                 //     * @param properties 字段属性，字段类型为UDCT时，需要设置key为customized_type_name的map
     */
    public ColumnInfo(String colName, String colType, int colIndex) {
        this.colName = colName;
        this.colType = FmdbTypeDesc.fromString(colType.toLowerCase());
        this.colIndex = colIndex;
//        this.properties = properties;
//        if (colType == FmdbDataType.UDCT) {
//            if (!properties.containsKey(Constant.UDCT_KEY)) {
//                throw new IllegalArgumentException("用户自定义字段类型时，需要通过字段属性来指定时间的数据类型");
//            }
//        }
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public String getColName() {
        return colName;
    }

    public void setColName(String colName) {
        this.colName = colName;
    }

    public FmdbTypeDesc getColType() {
        return colType;
    }

    public void setColType(String colType) {
        this.colType = FmdbTypeDesc.fromString(colType.toLowerCase());
    }

    public FmdbDataType getBaseType() {
        return baseType;
    }

    public void setBaseType(FmdbDataType baseType) {
        this.baseType = baseType;
    }

    public int getColIndex() {
        return colIndex;
    }

    public void setColIndex(int colIndex) {
        this.colIndex = colIndex;
    }

    public boolean isNull() {
        return isNull;
    }

    public void setNull(boolean aNull) {
        isNull = aNull;
    }

    public int getPrecision() {
        return precision;
    }

    public void setPrecision(int precision) throws Exception {
//        if (colType.equalsIgnoreCase("char") && precision > 256) {
//            throw new Exception("the precision of char need less than or equal 256");
//        }
        this.precision = precision;
    }

//    public Map<String, String> getProperties() {
//        return properties;
//    }
//
//    public void setProperties(Map<String, String> properties) {
//        if (colType == FmdbDataType.UDCT) {
//            if (!properties.containsKey(Constant.UDCT_KEY)) {
//                throw new IllegalArgumentException("用户自定义字段类型时，需要通过字段属性来指定时间的数据类型");
//            }
//        }
//        this.properties = properties;
//    }

    public int getScale() {
        return scale;
    }

    public void setScale(int scale) {
        this.scale = scale;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ColumnInfo that = (ColumnInfo) o;
        return colIndex == that.colIndex &&
                colName.equals(that.colName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(colName, colIndex);
    }
}
