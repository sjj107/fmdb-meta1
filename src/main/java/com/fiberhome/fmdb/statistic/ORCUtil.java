package com.fiberhome.fmdb.statistic;

import com.fiberhome.fmdb.common.Constant;
import com.fiberhome.fmdb.manifest.bean.OrcColInfo;
import com.fiberhome.fmdb.manifest.bean.OrcInfo;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.orc.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @Description 获取orc的元数据信息
 * @Author sjj
 * @Date 19/11/01 下午 03:55
 **/
public enum ORCUtil {
    INSTANCE;
    private static Logger logger = LoggerFactory.getLogger("fmdbmeta");
    //orc文件的路径
    private Reader reader;
    Configuration config = new Configuration();

    /**
     * 根据orc的writer获取orc的元数据信息
     * @param dbName 库名
     * @param tableName 表名
     * @param partName 分区名
     * @param orcName orc文件名
     * @param writer orc Writer
     * @return
     */
    public OrcInfo getOrcinfo(String dbName, String tableName, String partName, String orcName, Writer writer) {
        OrcInfo result = new OrcInfo(dbName, tableName, partName, orcName);
        long numberOfRows = writer.getNumberOfRows();
        result.setRows(numberOfRows);
        try {
            ColumnStatistics[] stats = writer.getStatistics();
            if (stats.length == 0) {
                return null;
            }
            result.setHasNull(stats[0].hasNull());
//        result.setCount(stats[0].getNumberOfValues());
            List<String> fieldNames = writer.getSchema().getFieldNames();
            ColumnStatistics statistics = null;
            Map<String, OrcColInfo> orcColInfos = Maps.newHashMap();
            OrcColInfo tmpInfo;
            for (int i = 1; i < stats.length; ++i) {
                tmpInfo = new OrcColInfo(dbName, tableName, partName, orcName, fieldNames.get(i - 1));
                statistics = stats[i];
                tmpInfo.setColName(fieldNames.get(i - 1));
                tmpInfo.setCount(statistics.getNumberOfValues());
//            tmpInfo.setBytesOnDisk(statistics.getBytesOnDisk());
                tmpInfo.setHasNull(statistics.hasNull());
                if (statistics instanceof IntegerColumnStatistics) {
                    IntegerColumnStatistics integerColumnStatistics = (IntegerColumnStatistics) statistics;
                    tmpInfo.setMin(integerColumnStatistics.getMinimum() + "");
                    tmpInfo.setMax(integerColumnStatistics.getMaximum() + "");
                } else if (statistics instanceof DateColumnStatistics) {
                    DateColumnStatistics dateColumnStatistics = (DateColumnStatistics) statistics;
                    tmpInfo.setMin(dateColumnStatistics.getMinimum() == null ? null : dateColumnStatistics.getMinimum() + "");
                    tmpInfo.setMax(dateColumnStatistics.getMaximum() == null ? null : dateColumnStatistics.getMaximum() + "");
                } else if (statistics instanceof TimestampColumnStatistics) {
                    TimestampColumnStatistics timestampColumnStatistics = (TimestampColumnStatistics) statistics;
                    tmpInfo.setMin(timestampColumnStatistics.getMinimum() == null ? null : timestampColumnStatistics.getMinimum() + "");
                    tmpInfo.setMax(timestampColumnStatistics.getMaximum() == null ? null : timestampColumnStatistics.getMaximum() + "");
                } else if (statistics instanceof DecimalColumnStatistics) {
                    DecimalColumnStatistics decimalColumnStatistics = (DecimalColumnStatistics) statistics;
                    tmpInfo.setMin(decimalColumnStatistics.getMinimum() == null ? null : decimalColumnStatistics.getMinimum() + "");
                    tmpInfo.setMax(decimalColumnStatistics.getMaximum() == null ? null : decimalColumnStatistics.getMaximum() + "");
                } else if (statistics instanceof DoubleColumnStatistics) {
                    DoubleColumnStatistics doubleColumnStatistics = (DoubleColumnStatistics) statistics;
                    tmpInfo.setMin(doubleColumnStatistics.getMinimum() + "");
                    tmpInfo.setMax(doubleColumnStatistics.getMaximum() + "");
                } else if (statistics instanceof StringColumnStatistics) {
                    StringColumnStatistics stringColumnStatistics = (StringColumnStatistics) statistics;
                    tmpInfo.setMin(stringColumnStatistics.getMinimum());
                    tmpInfo.setMax(stringColumnStatistics.getMaximum());
                } else {
                    tmpInfo.setMin("");
                    tmpInfo.setMax("");
                }
                orcColInfos.put(tmpInfo.getColName(), tmpInfo);
            }
            result.setColInfoMap(orcColInfos);
            return result;
        } catch (IOException e) {
            logger.error("获取统计信息失败", e);
            return null;
        }
    }

    /**
     * 根据orc文件的路径获取orc的元数据信息
     *
     * @param dataDir   数据目录
     * @param dbName    库名
     * @param tableName 表名
     * @param partName  分区名
     * @param orcName   orc文件名
     * @return
     */
    public OrcInfo getOrcinfo(String dataDir, String dbName, String tableName, String partName, String orcName) {
        String orcPath = dataDir + Constant.SLASH + dbName + Constant.SLASH + tableName + Constant.SLASH + partName + Constant.SLASH + orcName;
        try {
            reader = OrcFile.createReader(new Path(orcPath), OrcFile.readerOptions(config));
        } catch (IOException e) {
//            logger.error("初始化orc reader失败", e);
            return null;
        }
        //所有的字段名
        List<String> fieldNames = Lists.newArrayList();
        OrcInfo result = new OrcInfo(dbName, tableName, partName, orcName);
        long numberOfRows = reader.getNumberOfRows();
        result.setRows(numberOfRows);
        TypeDescription schema = reader.getSchema();
        fieldNames = schema.getFieldNames();
        ColumnStatistics[] stats = reader.getStatistics();
        if (stats.length == 0) {
            try {
                reader.close();
            } catch (IOException e) {
                logger.error("关闭[{}]的reader失败", orcPath);
            }
            return null;
        }
        result.setHasNull(stats[0].hasNull());
//        result.setCount(stats[0].getNumberOfValues());
        ColumnStatistics statistics = null;
        Map<String, OrcColInfo> orcColInfos = Maps.newHashMap();
        OrcColInfo tmpInfo;
        for (int i = 1; i < stats.length; ++i) {
            tmpInfo = new OrcColInfo(dbName, tableName, partName, orcName, fieldNames.get(i - 1));
            statistics = stats[i];
            tmpInfo.setColName(fieldNames.get(i - 1));
            tmpInfo.setCount(statistics.getNumberOfValues());
//            tmpInfo.setBytesOnDisk(statistics.getBytesOnDisk());
            tmpInfo.setHasNull(statistics.hasNull());
            if (statistics instanceof IntegerColumnStatistics) {
                IntegerColumnStatistics integerColumnStatistics = (IntegerColumnStatistics) statistics;
                tmpInfo.setMin(integerColumnStatistics.getMinimum() + "");
                tmpInfo.setMax(integerColumnStatistics.getMaximum() + "");
            } else if (statistics instanceof DateColumnStatistics) {
                DateColumnStatistics dateColumnStatistics = (DateColumnStatistics) statistics;
                tmpInfo.setMin(dateColumnStatistics.getMinimum() == null ? null : dateColumnStatistics.getMinimum() + "");
                tmpInfo.setMax(dateColumnStatistics.getMaximum() == null ? null : dateColumnStatistics.getMaximum() + "");
            } else if (statistics instanceof TimestampColumnStatistics) {
                TimestampColumnStatistics timestampColumnStatistics = (TimestampColumnStatistics) statistics;
                tmpInfo.setMin(timestampColumnStatistics.getMinimum() == null ? null : timestampColumnStatistics.getMinimum() + "");
                tmpInfo.setMax(timestampColumnStatistics.getMaximum() == null ? null : timestampColumnStatistics.getMaximum() + "");
            } else if (statistics instanceof DecimalColumnStatistics) {
                DecimalColumnStatistics decimalColumnStatistics = (DecimalColumnStatistics) statistics;
                tmpInfo.setMin(decimalColumnStatistics.getMinimum() == null ? null : decimalColumnStatistics.getMinimum() + "");
                tmpInfo.setMax(decimalColumnStatistics.getMaximum() == null ? null : decimalColumnStatistics.getMaximum() + "");
            } else if (statistics instanceof DoubleColumnStatistics) {
                DoubleColumnStatistics doubleColumnStatistics = (DoubleColumnStatistics) statistics;
                tmpInfo.setMin(doubleColumnStatistics.getMinimum() + "");
                tmpInfo.setMax(doubleColumnStatistics.getMaximum() + "");
            } else if (statistics instanceof StringColumnStatistics) {
                StringColumnStatistics stringColumnStatistics = (StringColumnStatistics) statistics;
                tmpInfo.setMin(stringColumnStatistics.getMinimum());
                tmpInfo.setMax(stringColumnStatistics.getMaximum());
            } else {
                tmpInfo.setMin("");
                tmpInfo.setMax("");
            }
            orcColInfos.put(tmpInfo.getColName(), tmpInfo);
        }
        result.setColInfoMap(orcColInfos);
        try {
            reader.close();
        } catch (IOException e) {
            logger.error("关闭[{}]的reader失败", orcPath);
        }
        return result;
    }

}
