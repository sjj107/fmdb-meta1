package com.fiberhome.fmdb.meta.test;

import com.fiberhome.fmdb.common.CommonUtil;
import com.fiberhome.fmdb.common.Constant;
import com.fiberhome.fmdb.manifest.bean.OrcInfo;
import com.fiberhome.fmdb.meta.bean.*;
import com.fiberhome.fmdb.meta.factory.FmdbMetaFactory;
import com.fiberhome.fmdb.meta.tool.IFMDBMetaClient;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by sjj on 19/10/16
 */
public class MetaClientTest {
    private static Logger logger = LoggerFactory.getLogger("fmdbmeta");
    private static IFMDBMetaClient metaClient;

    public static void main(String[] args) {
//        LoadConfFile.loadLog4j("conf/log4j.properties");
        logger.info("start...");
        metaClient = FmdbMetaFactory.INSTANCE.getMetaClient();
        metaClient = FmdbMetaFactory.INSTANCE.getMetaClient();
//        metaClient = FmdbMetaFactory.INSTANCE.getMetaClient();
//        listDb();
//        storageDB("fhorc");
//        deleteDB("fhorc");
//        storageUDCTTableTest("fhorc", "localtest");
        getTableInfo("fhorc", "localtest");
//       createUDCT();
//       getorcconf();
//        getUDCT("MAC");PGClient
//        deleteUDCT();
//            getAllUDCT();
//        checkData("fhorc", "fmdb_data_type_test_notnull",new String[]{"127","2","3","4","5","6","7","true","999","1010101010101"});
//        storageBaseTableTest("fhorc", "localtest");

//        deleteTable("fhorc", "localtest");
//        truncateTable("fhorc", "localtest");
//        getUDCTConfJson();
//        storageIndex();
//        deleteIndex();
//        getIndex();
//        getColType();
//        getOriColumnInfo();
//        getAllIndexName();
//        getAllTableName();
//        deleteTableManifest();
//        while (true){
//            getAllTableName();
//            try {
//                Thread.sleep(3000);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//        }
//        updateOrcStat();
//        metaClient.close();
    }

    private static void getUDCTConfJson() {
        String udctConfJson = metaClient.getUDCTConfJson();
    }

    private static void truncateTable(String dbName, String tableName) {
        metaClient.truncateTable(dbName, tableName);
    }

    private static void deleteUDCT() {
        Set<String> allUDCTNames = metaClient.getAllUDCTNames();
        for (String a : allUDCTNames) {
            boolean b = metaClient.deleteUDCT(a);
        }
    }

    private static void getAllUDCT() {
        Set<String> allUDCTNames = metaClient.getAllUDCTNames();
        System.out.println(allUDCTNames);
    }

    private static void getUDCT(String name) {
        UDCTInfo udctInfo = metaClient.getUDCTInfo(name);
        if (udctInfo == null) {
            System.out.println("不存在" + name);
        } else {
            System.out.println(udctInfo.getUdct_name());
            System.out.println(udctInfo.getRead());
            System.out.println(udctInfo.getWriter());
            System.out.println(udctInfo.getBase_type().getDesc());
        }

    }

    private static void createUDCT() {
        UDCTInfo tel = new UDCTInfo("tel", "org.apache.orc.impl.writer.MACTreeWriter", "org.apache.orc.impl.MACTreeReader", FmdbDataType.STRING);
        UDCTInfo mac = new UDCTInfo("mac", "org.apache.orc.impl.writer.MACTreeWriter", "org.apache.orc.impl.MACTreeReader", FmdbDataType.STRING);
        UDCTInfo carnum = new UDCTInfo("carnum", "org.apache.orc.impl.writer.CarNumTreeWriter", "org.apache.orc.impl.CarNumTreeReader", FmdbDataType.STRING);
        UDCTInfo email = new UDCTInfo("email", "org.apache.orc.impl.writer.EmailTreeWriter", "org.apache.orc.impl.EmailTreeReader", FmdbDataType.STRING);
        UDCTInfo idcard = new UDCTInfo("idcard", "org.apache.orc.impl.writer.IDCARDTreeWriter", "org.apache.orc.impl.IDCARDTreeReader", FmdbDataType.STRING);
        UDCTInfo ip = new UDCTInfo("ip", "org.apache.orc.impl.writer.IPTreeWriter", "org.apache.orc.impl.IPTreeReader", FmdbDataType.STRING);
/*        metaClient.creatUDCT(tel);
        metaClient.creatUDCT(mac);
        metaClient.creatUDCT(carnum);
        metaClient.creatUDCT(email);*/
        System.out.println(metaClient.creatUDCT(idcard));
        metaClient.creatUDCT(ip);
    }

    private static void checkData(String dbName, String tableName, String[] datas) {
        TableInfo tableInfo = metaClient.getTableInfo(dbName, tableName);
        CommonUtil.PutRecordMsg msg = CommonUtil.INSTANCE.checkRecord(datas, tableInfo);
        if (msg.isSuccess()) {
            System.out.println("成功");
        } else {
            System.out.println("失败原因" + msg.getFalseInfo());
        }
    }

    private static void updateOrcStat() {
        List<OrcInfo> falseL = Lists.newArrayList();
        OrcInfo orcInfo = new OrcInfo("newdb", "fborc", "all", "data_2.orc");
        OrcInfo orcInfo1 = new OrcInfo("newdb", "fborc", "all", "data_3.orc");
        falseL.add(orcInfo);
        falseL.add(orcInfo1);
        List<OrcInfo> trueL = Lists.newArrayList();
        OrcInfo orcInfo2 = new OrcInfo("newdb", "fborc", "all", "history_9.orc");
        OrcInfo orcInfo3 = new OrcInfo("newdb", "fborc", "all", "history_11.orc");
        trueL.add(orcInfo2);
        trueL.add(orcInfo3);
        boolean b = metaClient.changeOrcManifestStat(falseL, trueL);
        System.out.println("更新" + (b ? "成功" : "失败"));
    }


    private static void deleteTableManifest() {
        metaClient.deleteTableManifest("fhorc", "datatype");
    }

    private static void getAllTableName() {
        List<String> fhorc = metaClient.getAllTableName("fhorc");
        System.out.println(fhorc);
    }

    private static void getAllIndexName() {
        List<String> allIndexName = metaClient.getAllIndexName("fhorc", "datatype");
        System.out.println(allIndexName);
    }

//    private static void getOriColumnInfo() {
//        ColumnInfo oriColumnInfo = metaClient.getOriColumnInfo("fhorc", "datatype_index", "C1");
//        System.out.println(oriColumnInfo.getColType());
//    }

    private static void getColType() {
        String colType = metaClient.getColType("fhorc", "datatype", "C1");
        System.out.println(colType);
    }

    private static void listDb() {
        Set<String> list = metaClient.getAllDBNames();
        System.out.println(list);
    }

    private static void deleteDb() {
        metaClient.deleteDB("fhorc");
    }


    private static void getIndex() {
        IndexInfo indexInfo = metaClient.getIndexInfo("fhorc", "datatype_index");
        System.out.println(indexInfo.getCols());
        System.out.println(indexInfo.getIncludes());
    }

    private static void deleteIndex() {
        boolean b = metaClient.deleteIndex("fhorc", "datatype", "datatype_index");
    }

    private static void storageIndex() {
        IndexInfo indexInfo = new IndexInfo();
        indexInfo.setDbName("fhorc");
        indexInfo.setOrgTableName("datatype");
        indexInfo.setIndexName("datatype_index");
        indexInfo.setCols(Lists.newArrayList("C1"));
        indexInfo.setIncludes(Lists.newArrayList("C4"));
        metaClient.storageIndex(indexInfo);
    }

    private static void getTableInfo(String dbName, String tableName) {
        TableInfo tableInfo = metaClient.getTableInfo(dbName, tableName);
//        List<IndexInfo> indexs = tableInfo.getIndexs();
//        IndexInfo indexInfo = indexs.get(0);
//        String s = indexInfo.getCols().get(0);
//        ColumnInfo columnInfo = tableInfo.getColumnInfo(s);
        List<ColumnInfo> cols = tableInfo.getCols();
        System.out.println("字段信息：");
        for (ColumnInfo columnInfo : cols) {
            String colName = columnInfo.getColName();
            String colType = columnInfo.getColType().toString();
            int colIndex = columnInfo.getColIndex();
            FmdbDataType base_type = columnInfo.getBaseType();
//            String s = columnInfo.getProperties().get(Constant.UDCT_KEY);
            System.out.println("字段名：" + colName);
            System.out.println("字段类型：" + colType);
            System.out.println("字段下标：" + colIndex);
            System.out.println("字段基础类型：" + base_type.getDesc());
        }
        System.out.println("--------------------------------------------------");
        List<String> primaryKeys = tableInfo.getPrimaryKeys();
        System.out.println("主键：" + primaryKeys);
        PartitionInfo partition = tableInfo.getPartition();
        if (partition != null) {
            String col = partition.getCol();
            System.out.println("分区字段名:" + col);
            PartitionType partitionType = partition.getPartitionType();
            System.out.println("分区类型:" + partitionType);
        }
        int ttl = tableInfo.getTtl();
        System.out.println("周期:" + ttl);
        CompressionType compressionType = tableInfo.getCompressionType();
        System.out.println("压缩方式:" + compressionType);
        List<String> sortFields = tableInfo.getSortFields();
        System.out.println("排序字段：" + sortFields);
        String sortType = tableInfo.getSortType();
        System.out.println("排序类型：" + sortType);
        int orcSize = tableInfo.getOrcSize();
        System.out.println("orc文件大小：" + orcSize);

        System.out.println(tableInfo.getProperties());
//        System.out.println(columnInfo);
    }

    private static void listDB() {
        Set<String> list = metaClient.getAllDBNames();
        System.out.println(list);
    }

    private static void storageDB(String dbName) {
        metaClient.creatDB(dbName);
    }

    private static void deleteDB(String dbName) {
        metaClient.deleteDB(dbName);
    }

    private static void storageBaseTableTest(String dbName, String tableName) {
//        Map<String, String> pro = Maps.newHashMap();
//        pro.put(Constant.UDCT_KEY, "mac");
        //字段信息
        List<ColumnInfo> cols = new ArrayList<>();
        ColumnInfo columnInfo1 = new ColumnInfo();
        columnInfo1.setColName("f1");
        columnInfo1.setColIndex(0);
        columnInfo1.setColType("int");
        columnInfo1.setNull(false);

        ColumnInfo columnInfo2 = new ColumnInfo();
        columnInfo2.setColName("f2");
        columnInfo2.setColIndex(1);
        columnInfo2.setColType("int");

        ColumnInfo columnInfo3 = new ColumnInfo();
        columnInfo3.setColName("f3");
        columnInfo3.setColIndex(2);
        columnInfo3.setColType("int");

        cols.add(columnInfo1);
        cols.add(columnInfo2);
        cols.add(columnInfo3);

        Map<String, String> tablePro = new HashMap<>();
//        tablePro.put(Constant.PARTITION_NAME, "CATURE_TIME");
//        tablePro.put(Constant.PARTITION_TYPE, PartitionType.DAY.toString());
//        tablePro.put(Constant.TTL, "-1");
        tablePro.put(Constant.COMPRESSTYPE, "ZLIB");
        tablePro.put(Constant.SORT_FIELDS, "f1,f2");
        tablePro.put(Constant.SORT_TYPE, "asc");
//        tablePro.put(Constant.ORC_SIZE, "1024");
        TableInfo tableInfo = new TableInfo(dbName, tableName, cols, Lists.newArrayList("ID"));
//        tableInfo.setPartition(partitionInfo);
        tableInfo.setProperties(tablePro);
        boolean b = metaClient.storageTable(tableInfo);
        ColumnInfo columnInfo = tableInfo.getColumnInfo(1);
        System.out.println(b ? "成功" : "失败");
    }


    private static void storageUDCTTableTest(String dbName, String tableName) {
//        Map<String, String> pro = Maps.newHashMap();
//        pro.put(Constant.UDCT_KEY, "mac");
        //字段信息
        List<ColumnInfo> cols = new ArrayList<>();
        ColumnInfo columnInfo1 = new ColumnInfo();
        columnInfo1.setColName("f1");
        columnInfo1.setColIndex(0);
        columnInfo1.setColType("struct<f11:int,f12:int>");
        columnInfo1.setNull(false);

        cols.add(columnInfo1);

        Map<String, String> tablePro = new HashMap<>();
//        tablePro.put(Constant.PARTITION_NAME, "CATURE_TIME");
//        tablePro.put(Constant.PARTITION_TYPE, PartitionType.DAY.toString());
//        tablePro.put(Constant.TTL, "-1");
        tablePro.put(Constant.COMPRESSTYPE, "ZLIB");
        tablePro.put(Constant.SORT_FIELDS, "f1.f2");
        tablePro.put(Constant.SORT_TYPE, "desc");
//        tablePro.put(Constant.ORC_SIZE, "1024");
        TableInfo tableInfo = new TableInfo(dbName, tableName, cols, Lists.newArrayList("ID"));
//        tableInfo.setPartition(partitionInfo);
        tableInfo.setProperties(tablePro);
        boolean b = metaClient.storageTable(tableInfo);
        ColumnInfo columnInfo = tableInfo.getColumnInfo(1);
        System.out.println(b ? "成功" : "失败");
    }

    private static void deleteTable(String dbName, String tableName) {
        metaClient.deleteTable(dbName, tableName);
    }

    private static void getorcconf() {
        System.out.println("get");
        metaClient.getOrcConfiguration();
    }


}
