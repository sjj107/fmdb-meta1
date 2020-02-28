package com.fiberhome.fmdb.statistic;

import com.fiberhome.fmdb.common.CommandUtil;
import com.fiberhome.fmdb.common.Constant;
import com.fiberhome.fmdb.common.LoadConfFile;
import com.fiberhome.fmdb.statistic.bean.HumanBytes;
import com.fiberhome.fmdb.statistic.bean.MaxMin;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.orc.*;
import org.apache.orc.impl.OrcAcidUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.*;

/**
 * Created by sjj on 19/09/26
 * 统计表的信息：
 * 1、表的占用空间大小
 * 2、每列的占用空间
 * 3、每个文件的最大值和最小值
 */
public class ORCStatistic {
    private static Logger logger = LoggerFactory.getLogger("statistic");

    public static final PathFilter HIDDEN_AND_SIDE_FILE_FILTER = new PathFilter() {
        public boolean accept(Path p) {
            String name = p.getName();
            return !name.startsWith("_") && !name.startsWith(".") && !name.endsWith(
                    OrcAcidUtils.DELTA_SIDE_FILE_SUFFIX) && !name.contains(Constant.ORC_META_FILE_NAME);
        }
    };

    /**
     * 命令参数信息
     */
    private static Options options;
    private static CommandLineParser parser = new BasicParser();

    private static String orcFileDir;

    /**
     * 是否展示表占空间大小
     */
    private static boolean showTableSize;
    /**
     * 是否展示每列占的空间大小
     */
    private static boolean showColumnSize;
    /**
     * 是否展示每个文件中每个列的最小值最大值
     */
    private static boolean min_max;

    /**
     * 是否展示每个文件中的数据量
     */
    private static boolean dataNum;

    public static void main(String[] args) throws IOException, ClassNotFoundException {
//        LoadConfFile.loadLog4j(Constant.LOG4J_CONF_PATH);
        options = CommandUtil.getStatisticsOptions();
        try {

            CommandLine commandline = parser.parse(options, args);
            if (commandline.hasOption("h")) {
                showHelp();
                System.exit(0);
            }
            if (commandline.hasOption("f")) {
                orcFileDir = commandline.getOptionValue("f");
                if (orcFileDir == null) {
                    showHelp();
                    System.exit(0);
                }
            }
            showTableSize = commandline.hasOption("ts");
            showColumnSize = commandline.hasOption("cs");
            min_max = commandline.hasOption("mm");
            dataNum = commandline.hasOption("dn");
            if (!(showTableSize || showColumnSize || min_max || dataNum)) {
                showHelp();
                System.exit(0);
            }
        } catch (ParseException e) {
            System.err.println("解析命令报错");
            e.printStackTrace();
        }
        showStatistics(orcFileDir);
//        showStatistics("E:\\data\\orc\\data\\fborc");
    }

    /**
     * 显示帮助信息
     */
    private static void showHelp() {
        HelpFormatter helpFormatter = new HelpFormatter();
        helpFormatter.printHelp("sh showstatistic.sh [option]", options);
        logger.info("--------------------------------------------------------------------------------");
        logger.info("case 1: sh showstatistic.sh -f /home/data/tableDir -ts -cs -dn -mm");
    }

    /**
     * 统计ORC文件的大小
     *
     * @return
     */
    public static long getORCSize(String orcPath) {
        Configuration config = new Configuration();
        Path file = new Path(orcPath);
        FileSystem fs;
        long fileLen = 0;
        try {
            fs = file.getFileSystem(config);
            fileLen = fs.getFileStatus(file).getLen();
        } catch (IOException e1) {
            e1.printStackTrace();
        }
//    	Reader reader = null;
//    	try {
//			reader = OrcFile.createReader(new Path(orcPath), OrcFile.readerOptions(config));
//		} catch (IllegalArgumentException | IOException e) {
//			e.printStackTrace();
//		}
//    	long fileLen = fs.getFileStatus(file).getLen();
        return fileLen;
    }

    /**
     * 展示统计信息
     *
     * @param tableName
     */
    public static void showStatistics(String tableName) throws IOException {
        //表占空间大小
        HumanBytes tableTotalSize = new HumanBytes(0);
        //每列占的空间大小<字段名，大小>
        Map<String, HumanBytes> colSize = Maps.newHashMap();
        //每个文件中每个列的最大值最小值
        Table<String, String, MaxMin> colMM = HashBasedTable.create();
        //每个orc文件中数据的量
        Map<String, Long> fileNums = Maps.newHashMap();
        Configuration config = new Configuration();
        //当前表的路径
        Path path = new Path(tableName);
        //所有的orc文件名
        List<String> filesInPath = new ArrayList<>();
        //所有的字段名
        List<String> fieldNames = Lists.newArrayList();
        filesInPath.addAll(getAllFilesInPath(path, config));
        Reader reader = null;
        for (String filename : filesInPath) {
            //叠加orc文件的空间大小
            Path file = new Path(filename);
            FileSystem fs = file.getFileSystem(config);
            long len = fs.getFileStatus(file).getLen();
            tableTotalSize.setOrigSize(tableTotalSize.getOrigSize() + len);
            //统计每个列的占空间大小
            reader = OrcFile.createReader(new Path(filename), OrcFile.readerOptions(config));
            //每个orc文件的数量
            long numberOfRows = reader.getNumberOfRows();
            fileNums.put(filename, numberOfRows);
            TypeDescription schema = reader.getSchema();
            fieldNames = schema.getFieldNames();
            ColumnStatistics[] stats = reader.getStatistics();
            for (int i = 1; i < stats.length; ++i) {
                ColumnStatistics statistics = stats[i];
                long bytesOnDisk = statistics.getBytesOnDisk();
                if (colSize.get(fieldNames.get(i - 1)) != null) {
                    colSize.put(fieldNames.get(i - 1), new HumanBytes(colSize.get(fieldNames.get(i - 1)).getOrigSize() + bytesOnDisk));
                } else {
                    colSize.put(fieldNames.get(i - 1), new HumanBytes(bytesOnDisk));
                }
                //统计每个文件中各个字段的最大和最小值
                if (statistics instanceof IntegerColumnStatistics) {
                    IntegerColumnStatistics integerColumnStatistics = (IntegerColumnStatistics) statistics;
                    long minimum = integerColumnStatistics.getMinimum();
                    long maximum = integerColumnStatistics.getMaximum();
                    colMM.put(filename, fieldNames.get(i - 1), new MaxMin(maximum, minimum));
                } else if (statistics instanceof BinaryColumnStatistics) {
//                    BinaryColumnStatistics binaryColumnStatistics = (BinaryColumnStatistics) statistics;
//                    binaryColumnStatistics.getMinimum();
//                    binaryColumnStatistics.getMaximum();
//                    colMM.put(filename,fieldNames.get(i - 1),new MaxMin(maximum,minimum));
                } else if (statistics instanceof BooleanColumnStatistics) {
//                    BooleanColumnStatistics booleanColumnStatistics = (BooleanColumnStatistics)statistics;

                } else if (statistics instanceof CollectionColumnStatistics) {
                    CollectionColumnStatistics collectionColumnStatistics = (CollectionColumnStatistics) statistics;
//                    collectionColumnStatistics.ge
                } else if (statistics instanceof DateColumnStatistics) {
                    DateColumnStatistics dateColumnStatistics = (DateColumnStatistics) statistics;
                    Date maximum = dateColumnStatistics.getMaximum();
                    Date minimum = dateColumnStatistics.getMinimum();
                    colMM.put(filename, fieldNames.get(i - 1), new MaxMin(maximum, minimum));
                } else if (statistics instanceof TimestampColumnStatistics) {
                    TimestampColumnStatistics timestampColumnStatistics = (TimestampColumnStatistics) statistics;
                    Timestamp maximum = timestampColumnStatistics.getMaximum();
                    Timestamp minimum = timestampColumnStatistics.getMinimum();
                    colMM.put(filename, fieldNames.get(i - 1), new MaxMin(maximum, minimum));
                } else if (statistics instanceof DecimalColumnStatistics) {
                    DecimalColumnStatistics decimalColumnStatistics = (DecimalColumnStatistics) statistics;
                    HiveDecimal minimum = decimalColumnStatistics.getMinimum();
                    HiveDecimal maximum = decimalColumnStatistics.getMaximum();
                    colMM.put(filename, fieldNames.get(i - 1), new MaxMin(maximum, minimum));
                } else if (statistics instanceof DoubleColumnStatistics) {
                    DoubleColumnStatistics doubleColumnStatistics = (DoubleColumnStatistics) statistics;
                    double maximum = doubleColumnStatistics.getMaximum();
                    double minimum = doubleColumnStatistics.getMinimum();
                    colMM.put(filename, fieldNames.get(i - 1), new MaxMin(maximum, minimum));
                } else if (statistics instanceof StringColumnStatistics) {
                    StringColumnStatistics stringColumnStatistics = (StringColumnStatistics) statistics;
                    String maximum = stringColumnStatistics.getMaximum();
                    String minimum = stringColumnStatistics.getMinimum();
                    colMM.put(filename, fieldNames.get(i - 1), new MaxMin(maximum, minimum));
                } else if (statistics instanceof TimestampColumnStatistics) {
                    TimestampColumnStatistics timestampColumnStatistics = (TimestampColumnStatistics) statistics;
                }
            }

        }
        if (showTableSize) {
            logger.info("表占空间大小：" + tableTotalSize);
            logger.info("----------------------------------------------");
        }
        if (showColumnSize) {
            logger.info("每列占的空间大小<字段名，大小>：");
            showSize(colSize);
            logger.info("----------------------------------------------");
        }
        if (min_max) {
            logger.info("每个文件中每个列的最小值最大值：");
            showTable(colMM);
            logger.info("----------------------------------------------");
        }
        if (dataNum) {
            logger.info("每个文件中的数据量：");
            showMap(fileNums);
            logger.info("----------------------------------------------");
        }
    }

    /**
     * 每个文件中每个列的最小值最大值
     *
     * @param colMM
     */
    private static void showTable(Table<String, String, MaxMin> colMM) {
        Set<String> strings = colMM.rowKeySet();
        Map<String, MaxMin> column;
        for (String s : strings) {
            logger.info("文件：" + s);
            column = colMM.row(s);
            Set<String> strings1 = column.keySet();
            for (String s1 : strings1) {
                MaxMin maxMin = column.get(s1);
                logger.info(s1 + "\t" + maxMin);
            }
        }
    }

    /**
     * 每列占的空间大小<字段名，大小>
     *
     * @param map canshu
     */
    private static void showSize(Map<String, HumanBytes> map) {
        HumanBytes tmp;
        Set<String> strings = map.keySet();
        for (String s : strings) {
            tmp = map.get(s);
            logger.info(s + "\t" + tmp);
        }
    }

    /**
     * 每个文件中的数据量,以及总数
     *
     * @param map
     */
    private static void showMap(Map<String, Long> map) {
        long total = 0;
        long tmp;
        Set<String> strings = map.keySet();
        for (String s : strings) {
            tmp = map.get(s);
            total = total + tmp;
            logger.info(s + "\t" + tmp);
        }
        logger.info("total" + "\t" + total);
    }

    public static Collection<String> getAllFilesInPath(final Path path,
                                                       final Configuration conf) throws IOException {
        List<String> filesInPath = new ArrayList<>();
        FileSystem fs = path.getFileSystem(conf);
        FileStatus fileStatus = fs.getFileStatus(path);
        if (fileStatus.isDir()) {
            FileStatus[] fileStatuses = fs.listStatus(path, HIDDEN_AND_SIDE_FILE_FILTER);
            for (FileStatus fileInPath : fileStatuses) {
                if (fileInPath.isDir()) {
                    filesInPath.addAll(getAllFilesInPath(fileInPath.getPath(), conf));
                } else {
                    filesInPath.add(fileInPath.getPath().toString());
                }
            }
        } else {
            filesInPath.add(path.toString());
        }

        return filesInPath;
    }
}

