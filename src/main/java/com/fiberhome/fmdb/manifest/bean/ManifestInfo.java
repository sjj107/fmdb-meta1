package com.fiberhome.fmdb.manifest.bean;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * @Description 对应表的元数据信息
 * @Author sjj
 * @Date 19/11/15 上午 10:04
 **/
public class ManifestInfo implements Serializable {
    private static final long serialVersionUID = -2452611679327586295L;
    //    /**
//     * 表包含的orc信息
//     */
//    private List<OrcInfo> orcInfos = Lists.newArrayList();
    //<orcname,OrcInfo>
    private Map<String, OrcInfo> orcInfoMap = Maps.newHashMap();
    /**
     * 表的数据量
     */
    private long dataCount = 0;

    public ManifestInfo() {
    }

//    public List<OrcInfo> getOrcInfos() {
//        return orcInfos;
//    }
//
//    public void setOrcInfos(List<OrcInfo> orcInfos) {
//        this.orcInfos = orcInfos;
//    }

    public void addOrcInfo(OrcInfo orcInfo) {
//        this.orcInfos.add(orcInfo);
        if (this.orcInfoMap.containsKey(orcInfo.getName())) {
            this.orcInfoMap.put(orcInfo.getName(), orcInfo);
        } else {
            this.orcInfoMap.put(orcInfo.getName(), orcInfo);
            this.dataCount += orcInfo.getRows();
        }


    }

    public Map<String, OrcInfo> getOrcInfoMap() {
        return orcInfoMap;
    }

    public long getDataCount() {
        return dataCount;
    }

    public void setDataCount(long dataCount) {
        this.dataCount = dataCount;
    }

    @Override
    public String toString() {
        return "ManifestInfo{" +
                "orcInfoMap=" + orcInfoMap +
                ", dataCount=" + dataCount +
                '}';
    }
}
