package com.fiberhome.fmdb.meta.test;

import com.fiberhome.fmdb.common.Constant;
import com.fiberhome.fmdb.manifest.OrcMetaClient.IOrcMetaCache;
import com.fiberhome.fmdb.manifest.OrcMetaClient.impl.LocalOrcMetaCacheImp;
import com.fiberhome.fmdb.common.LoadConfFile;
import com.fiberhome.fmdb.manifest.bean.*;
import com.fiberhome.fmdb.manifest.factory.OrcMetaCacheFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by sjj on 19/10/21
 */
public class OrcMetaCacheTest {
    public static void main(String[] args) {
//        LoadConfFile.loadLog4j(Constant.LOG4J_CONF_PATH);
        IOrcMetaCache orcMetaCache = OrcMetaCacheFactory.INSTANCE.getOrcMetaCache();
        TableManifestInfo manifestInfo = orcMetaCache.getManifestInfo("fhorc", "orc_http_4500w_all");
        long count = manifestInfo.getCount();
        System.out.println("数据量:" + count);
        Map<String, ManifestInfo> manifestInfoMap = manifestInfo.getManifestInfoMap();
        Set<String> strings = manifestInfoMap.keySet();
        for (String part : strings) {
            System.out.println("分区：" + part);
            ManifestInfo manifestInfo1 = manifestInfoMap.get(part);
            long dataCount = manifestInfo1.getDataCount();
            System.out.println("数据量:" + dataCount);
            Map<String, OrcInfo> orcInfoMap = manifestInfo1.getOrcInfoMap();
            Set<String> strings1 = orcInfoMap.keySet();
            for (String orcname : strings1) {
                System.out.println("orc名：" + orcname);
                OrcInfo orcInfo = orcInfoMap.get(orcname);
                long rows = orcInfo.getRows();
                System.out.println("数据量:" + rows);
                Map<String, OrcColInfo> colInfoMap = orcInfo.getColInfoMap();
                for (OrcColInfo orcColInfo : colInfoMap.values()) {
                    String colName = orcColInfo.getColName();
                    System.out.println("字段名：" + colName);
                    String min = orcColInfo.getMin();
                    System.out.println("最小值：" + min);
                    String max = orcColInfo.getMax();
                    System.out.println("最大值：" + max);
                    long count1 = orcColInfo.getCount();
                    System.out.println("数据量:" + count1);
                    long nullCount = orcColInfo.getNullCount();
                    System.out.println("nullCount数据量:" + nullCount);
                }
            }
        }
//        while (true) {
//            Map<String, DataRange> columnRange = orcMetaCache.getColumnRange("fborc", "partitiontable", "CATURE_TIME");
//            System.out.println(columnRange);
//            try {
//                Thread.sleep(5000);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//        }

    }
}
