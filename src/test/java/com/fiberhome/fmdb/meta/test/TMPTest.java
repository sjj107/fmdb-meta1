package com.fiberhome.fmdb.meta.test;

import com.fiberhome.fmdb.common.Constant;
import com.fiberhome.fmdb.zookeeper.ZKManager;
import com.google.common.base.Joiner;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Table;
import org.apache.zookeeper.KeeperException;

import java.io.File;
import java.util.List;

/**
 * Created by sjj on 19/10/23
 */
public class TMPTest {
    public static void main(String[] args) throws KeeperException, InterruptedException {
        ZKManager zookeeper = ZKManager.getInstance();
//        zookeeper.createPersistentMultiPath("/FMDB/deleted/table/dd","data".getBytes());
        zookeeper.deleteNode("/FMDB/deleted/table/dd");
//        long l = Long.MIN_VALUE;
//        System.out.println(Float.MIN_VALUE);
//        BigDecimal min = new BigDecimal(Float.MIN_VALUE);
//        BigDecimal tmp = new BigDecimal(-2.1F);
//        BigDecimal tmp2 = new BigDecimal(2.1F);
//        System.out.println(min.compareTo(tmp2));
//        String data = "77.7";
//        System.out.println();//3.4028235E38
//        System.out.println(BigDecimal.valueOf(Double.parseDouble(data)).doubleValue());
//        System.out.println(Float.MAX_VALUE);
//        manifestinfotest();
//        tbaleTest();
//        fileTest();
    }
    public static void fileTest(){
        File file = new File("E:\\data\\orc\\data0.1.0\\fhorc\\partitiontable\\partitiontable-20191112");
        File[] files = file.listFiles(Constant.ORC_TMP_FILE);
        System.out.println(files);
    }

    public static void tbaleTest(){
        Table<String, String, String> table = HashBasedTable.create();
        table.put("C1","1:2","orc1");
        table.put("C1","1:2","orc2");
        String c1 = table.get("C1", "1:2");
        System.out.println(c1);
    }

//    public static void manifestinfotest() {
//        List<ManifestInfo> list = Lists.newArrayList();
//        ManifestInfo manifestInfo = new ManifestInfo("fborc", "datatype", "data_1.0rc");
//        manifestInfo.setColName("C1");
//        manifestInfo.setCount(2);
//        manifestInfo.setMin("1");
//        manifestInfo.setMax("9");
//        list.add(manifestInfo);
//
//        ManifestInfo manifestInfo1 = new ManifestInfo("fborc", "datatype", "data_1.0rc");
//        manifestInfo1.setColName("C2");
//        manifestInfo1.setCount(2);
//        manifestInfo1.setMin("1");
//        manifestInfo1.setMax("9");
//        if (list.contains(manifestInfo1)) {
//            System.out.println("包含");
//        } else {
//            System.out.println("不包含");
//        }
//    }

    public static void jiontest() {
        List<String> primaryKeys = Lists.newArrayList();
        String join = Joiner.on(",").join(primaryKeys);
        System.out.println(join);
    }
}
