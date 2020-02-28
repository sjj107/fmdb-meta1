package com.fiberhome.fmdb.manifest.bean;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * @Description 记录orc的info
 * @Author sjj
 * @Date 19/11/01 下午 04:02
 **/
public class OrcInfo implements Serializable {
    private static final long serialVersionUID = 2496399207816526369L;
    private String dbName;
    private String tblName;
    private String partName;
    //orc的文件名
    private String name;
    //数据条数
    private long rows;
    //    //不为空的数据条数
//    private long count;
    //是否存在空值
    private boolean hasNull;
    //所有的字段信息
    private Map<String, OrcColInfo> colInfoMap = Maps.newHashMap();

    private OrcInfo() {
    }

    public OrcInfo(String dbName, String tblName, String partName, String name) {
        this.dbName = dbName;
        this.tblName = tblName;
        this.partName = partName;
        this.name = name;
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

    public String getName() {
        return name;
    }

    public long getRows() {
        return rows;
    }

    public void setRows(long rows) {
        this.rows = rows;
    }

    public boolean isHasNull() {
        return hasNull;
    }

    public void setHasNull(boolean hasNull) {
        this.hasNull = hasNull;
    }

    public Map<String, OrcColInfo> getColInfoMap() {
        return colInfoMap;
    }

    public void setColInfoMap(Map<String, OrcColInfo> colInfoMap) {
        this.colInfoMap = colInfoMap;
    }

    public void addColInfo(OrcColInfo orcColInfo) {
        this.colInfoMap.put(orcColInfo.getColName(), orcColInfo);
        this.rows = orcColInfo.getCount() + orcColInfo.getNullCount();
    }

//    public long getCount() {
//        return count;
//    }
//
//    public void setCount(long count) {
//        this.count = count;
//    }

    @Override
    public String toString() {
        return "OrcInfo{" +
                "name='" + name + '\'' +
                ", rows=" + rows +
                ", hasNull=" + hasNull +
                ", colInfoMap=" + colInfoMap +
                '}';
    }
}
