package com.fiberhome.fmdb.manifest.bean;

import com.google.common.collect.Maps;

import java.io.Serializable;
import java.util.Map;

/**
 * @Description 表的元数据信息
 * @Author sjj
 * @Date 19/11/18 下午 05:17
 **/
public class TableManifestInfo implements Serializable {
    private static final long serialVersionUID = 8266008978856365787L;
    //库名
    private String dbName;
    //表名
    private String tableNmae;

    private long count = 0;
    /**
     * 表的元数据信息<br>
     * <分取名，分区的manifest信息>
     */
    private Map<String, ManifestInfo> manifestInfoMap = Maps.newHashMap();

    //    private
    public TableManifestInfo(String dbName, String tableNmae) {
        this.dbName = dbName;
        this.tableNmae = tableNmae;
    }

    public Map<String, ManifestInfo> getManifestInfoMap() {
        return manifestInfoMap;
    }

    public void addOrcColInfo(OrcColInfo orcColInfo) {
        String dbName = orcColInfo.getDbName();
        String tblName = orcColInfo.getTblName();
        String partName = orcColInfo.getPartName();
        ManifestInfo manifestInfo = manifestInfoMap.get(partName);
        if (manifestInfo == null) {
            manifestInfo = new ManifestInfo();
            OrcInfo orcInfo = new OrcInfo(dbName, tblName, partName, orcColInfo.getOrcName());
//            orcInfo.setRows(orcColInfo.getCount() + orcColInfo.getNullCount());
            orcInfo.addColInfo(orcColInfo);
            manifestInfo.addOrcInfo(orcInfo);
            this.count += manifestInfo.getDataCount();
        } else {
            OrcInfo orcInfo = manifestInfo.getOrcInfoMap().get(orcColInfo.getOrcName());
            if (orcInfo == null) {
                orcInfo = new OrcInfo(dbName, tblName, partName, orcColInfo.getOrcName());
//                orcInfo.setRows(orcColInfo.getCount() + orcColInfo.getNullCount());
            }
            orcInfo.addColInfo(orcColInfo);
            manifestInfo.addOrcInfo(orcInfo);
        }
        manifestInfoMap.put(partName, manifestInfo);

    }
//    public void setManifestInfoMap(Map<String, ManifestInfo> manifestInfoMap) {
//        this.manifestInfoMap = manifestInfoMap;
//    }

    public void addManifestInfo(String partName, ManifestInfo manifestInfo) {

        this.manifestInfoMap.put(partName, manifestInfo);
        this.count += manifestInfo.getDataCount();
    }

    public long getCount() {
        return count;
    }

    @Override
    public String toString() {
        return "TableManifestInfo{" +
                "dbName='" + dbName + '\'' +
                ", tableNmae='" + tableNmae + '\'' +
                ", count=" + count +
                ", manifestInfoMap=" + manifestInfoMap +
                '}';
    }
}
