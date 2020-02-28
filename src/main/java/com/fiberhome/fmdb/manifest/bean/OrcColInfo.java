package com.fiberhome.fmdb.manifest.bean;

import java.io.Serializable;

/**
 * @Description orc文件中每个字段的信息
 * @Author sjj
 * @Date 19/11/01 下午 04:05
 **/
public class OrcColInfo implements Serializable {
    private static final long serialVersionUID = -5670302211425090015L;
    private String dbName;
    private String tblName;
    private String partName;
    private String orcName;
    //字段名
    private String colName;

    private OrcColInfo() {
    }

    public OrcColInfo(String dbName, String tblName, String partName, String orcName, String colName) {
        this.dbName = dbName;
        this.tblName = tblName;
        this.partName = partName;
        this.orcName = orcName;
        this.colName = colName;
    }

    public String getDbName() {
        return dbName;
    }

    public String getTblName() {
        return tblName;
    }

    public String getPartName() {
        return partName;
    }

    public String getOrcName() {
        return orcName;
    }
    //不为空的数据条数
    private long count;
    //是否有空值
    private boolean hasNull;
    //为null值得数量
    private long nullCount;
//    //所占磁盘大小
//    private long bytesOnDisk;
    //最小值
    private String min;
    //最大值
    private String max;

    private boolean stat;

    public String getColName() {
        return colName;
    }

    public void setColName(String colName) {
        this.colName = colName;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public boolean isHasNull() {
        return hasNull;
    }

    public void setHasNull(boolean hasNull) {
        this.hasNull = hasNull;
    }

//    public long getBytesOnDisk() {
//        return bytesOnDisk;
//    }


    public boolean isStat() {
        return stat;
    }

    public void setStat(boolean stat) {
        this.stat = stat;
    }

    public long getNullCount() {
        return nullCount;
    }

    public void setNullCount(long nullCount) {
        this.nullCount = nullCount;
    }

//    public void setBytesOnDisk(long bytesOnDisk) {
//        this.bytesOnDisk = bytesOnDisk;
//    }

    public String getMin() {
        return min;
    }

    public void setMin(String min) {
        this.min = min;
    }

    public String getMax() {
        return max;
    }

    public void setMax(String max) {
        this.max = max;
    }

    @Override
    public String toString() {
        return "OrcColInfo{" +
                "colName='" + colName + '\'' +
                ", count=" + count +
                ", hasNull=" + hasNull +
                ", nullCount=" + nullCount +
                ", min='" + min + '\'' +
                ", max='" + max + '\'' +
                '}';
    }
}
