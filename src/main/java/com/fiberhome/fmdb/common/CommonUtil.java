package com.fiberhome.fmdb.common;

import com.alibaba.druid.pool.DruidDataSourceFactory;
import com.fiberhome.fmdb.meta.bean.ColumnInfo;
import com.fiberhome.fmdb.meta.bean.FmdbDataType;
import com.fiberhome.fmdb.meta.bean.PartitionType;
import com.fiberhome.fmdb.meta.bean.TableInfo;
import com.google.common.collect.Sets;
import org.apache.hadoop.hive.ql.exec.vector.*;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.*;
import java.util.regex.Pattern;

/**
 * Created by sjj on 19/10/17
 * 通用工具类
 */
public enum CommonUtil {
    INSTANCE;
    private static Logger logger = LoggerFactory.getLogger("fmdbmeta");
    private static Random random = new Random();

    /**
     * 根据字段信息，获取orc的schema
     *
     * @param cols 字段信息
     * @return orc的schema
     */
    public String genTableStruct(List<ColumnInfo> cols) {
        StringBuilder stringBuilder = new StringBuilder("struct<");
        for (ColumnInfo columnInfo : cols) {
            stringBuilder.append(columnInfo.getColName()).append(":").append(getColStruct(columnInfo)).append(",");
        }
        stringBuilder.deleteCharAt(stringBuilder.length() - 1);
        stringBuilder.append(">");
        return stringBuilder.toString();
    }

    /**
     * 根据字段信息生成orc支持的struct
     *
     * @param columnInfo 字段信息
     * @return orc支持的struct
     */
    private String getColStruct(ColumnInfo columnInfo) {
        switch (columnInfo.getColType().toLowerCase()) {
            case "decimal":
                return columnInfo.getColType() + Constant.LEFT_BRACKET + columnInfo.getPrecision() + Constant.COMMA + columnInfo.getScale() + Constant.RIGHT_BRACKET;
            case "char":
            case "varchar":
                return columnInfo.getColType() + Constant.LEFT_BRACKET + columnInfo.getPrecision() + Constant.RIGHT_BRACKET;
            default:
                return columnInfo.getColType();
        }
//        switch (columnInfo.getBaseType()) {
//            case BOOLEAN:
//            case TINYINT:
//            case SMALLINT:
//            case INT:
//            case BIGINT:
//            case FLOAT:
//            case DOUBLE:
//            case STRING:
//            case DATE:
//            case TIMESTAMP:
//            case BINARY:
//                return columnInfo.getBaseType().getDesc();
//            case DECIMAL:
//                return columnInfo.getBaseType().getDesc() + Constant.LEFT_BRACKET + columnInfo.getPrecision() + Constant.COMMA + columnInfo.getScale() + Constant.RIGHT_BRACKET;
//            case CHAR:
//            case VARCHAR:
//                return columnInfo.getBaseType().getDesc() + Constant.LEFT_BRACKET + columnInfo.getPrecision() + Constant.RIGHT_BRACKET;
//            default:
//                throw new IllegalArgumentException("当前不支持该类型:" + columnInfo.getColType());
//        }
    }

    /**
     * 获取系统类型
     *
     * @return 系统类型
     */
    public String getSystemName() {
        return System.getProperties().getProperty("os.name");
    }

    /**
     * 生成ID
     *
     * @return id
     */
    public String genID() {
        String format = String.format("%04d", random.nextInt(10000));
        return System.currentTimeMillis() + format;
    }

    /**
     * 将orc的临时文件重命名为最终文件
     *
     * @param orcTmpFile
     * @return
     */
    public boolean reNameOrcTmpFile(File orcTmpFile) {
        if (!orcTmpFile.getName().startsWith(Constant.ORC_FILE_NAME_PREFIX) || !orcTmpFile.getName().endsWith(Constant.ORC_TMP_FILE_NAME_SUFFIX)) {
            logger.error("传入的文件不是orc的临时文件");
            return false;
        }
        boolean b = orcTmpFile.renameTo(new File(orcTmpFile.getAbsolutePath().replace(Constant.ORC_TMP_FILE_NAME_SUFFIX, Constant.ORC_FILE_NAME_SUFFIX)));
        return b;
    }

    /**
     * 判断orc数据目录下是否为orc的临时文件
     *
     * @param dataDirS 数据目录
     * @return
     */
    public boolean containsOrcTmpFile(String dataDirS) {
        File dataDir = new File(dataDirS);
        if (!dataDir.exists() || !dataDir.isDirectory()) {
            return false;
        }
        File[] dbDirs = dataDir.listFiles();
        for (File dbDir : dbDirs) {
            if (!dbDir.exists() || !dbDir.isDirectory()) {
                continue;
            }
            File[] tableDirs = dbDir.listFiles();
            for (File tableDir : tableDirs) {
                if (!tableDir.exists() || !tableDir.isDirectory()) {
                    continue;
                }
                File[] partDirs = tableDir.listFiles();
                for (File partDir : partDirs) {
                    if (!partDir.exists() || !partDir.isDirectory()) {
                        continue;
                    }
                    File[] files = partDir.listFiles(Constant.ORC_TMP_FILE);
                    if (files.length > 0) {
                        return true;
                    }
                }

            }
        }
        return false;
    }


    /**
     * 获取PG的连接
     *
     * @param pg_ip
     * @param pg_port
     * @param pg_dataBase
     * @param pg_user
     * @param pg_psw
     * @return
     */
    public Connection getPGConnection(String pg_ip, int pg_port, String pg_dataBase, String pg_user, String pg_psw) {
        Connection connection = null;
        try {
            Class.forName(Constant.PG_DRIVER_CLASS);
        } catch (ClassNotFoundException e) {
            logger.error("加载驱动错误[{}]", Constant.PG_DRIVER_CLASS, e);
        }
        try {
            logger.debug("连接信息：[{}]", Constant.PG_JDBC_URL_PRE + pg_ip + Constant.COLON + pg_port + pg_dataBase);
            connection = DriverManager.getConnection(Constant.PG_JDBC_URL_PRE + pg_ip + Constant.COLON + pg_port + pg_dataBase, pg_user, pg_psw);
            connection.setAutoCommit(false);
        } catch (SQLException e) {
            logger.error("连接PG错误", e);
        }
        return connection;
    }

    /**
     * 获取连接池
     *
     * @param driveClass 驱动
     * @param url        url
     * @param userName   用户名
     * @param userPSW    密码
     * @return 连接池
     */
    public DataSource getDataSource(String driveClass, String url, String userName, String userPSW) {
        DataSource dataSource = null;
        Properties properties = new Properties();
        properties.setProperty("driverClassName", driveClass);
        properties.setProperty("url", url);
        properties.setProperty("username", userName);
        properties.setProperty("password", userPSW);
        properties.setProperty("initialSize", Constant.DS_INITIALSIZE);
        properties.setProperty("maxActive", Constant.DS_MAXACTIVE);
        properties.setProperty("maxWait", Constant.DS_MAXWAIT);
        properties.setProperty("minIdle", Constant.DS_MINIDLE);
        try {
            dataSource = DruidDataSourceFactory.createDataSource(properties);
        } catch (Exception e) {
            logger.error("获取连接池失败", e);
        }
        return dataSource;
    }

    /**
     * 对数据的校验结果
     */
    public class PutRecordMsg {
        boolean isSuccess = true;
        String falseInfo = "我没错";

        public boolean isSuccess() {
            return isSuccess;
        }

        public void setSuccess(boolean success) {
            isSuccess = success;
        }

        public String getFalseInfo() {
            return falseInfo;
        }

        public void setFalseInfo(String falseInfo) {
            this.falseInfo = falseInfo;
        }
    }

    /**
     * 将字符串数组中的空字符串转成null
     *
     * @param array
     * @return
     */
    public void convertEmptyStringToNull(String[] array) {
        for (int i = 0; i < array.length; i++) {
            if (array[i] != null && array[i].equalsIgnoreCase("")) {
                array[i] = null;
            }
        }
    }

    public PutRecordMsg checkRecord(String[] datas, TableInfo tableInfo) {
        PutRecordMsg msg = new PutRecordMsg();
//        int index = 0;
        if (datas.length != tableInfo.getCols().size()) {
            msg.setSuccess(false);
            msg.setFalseInfo("字段个数错误");
            return msg;
        }
        long currentLong = 0;
        double currentDouble = 0;
        ColumnInfo columnInfo;
        String colType;
        for (int index = 0; index < datas.length; index++) {
            columnInfo = tableInfo.getColumnInfo(index);
            colType = columnInfo.getColType();
            if (datas[index] == null) {
                if (!columnInfo.isNull()) {
                    msg.setSuccess(false);
                    msg.setFalseInfo(String.format("第[%d]字段不能为空", index));
                    return msg;
                }
            }
            if (datas[index] != null) {
                if (colType.equalsIgnoreCase("tinyint") || colType.equalsIgnoreCase("smallint")
                        || colType.equalsIgnoreCase("int") || colType.equalsIgnoreCase("bigint")) {
//                    boolean matches = Pattern.matches(Constant.LONG_PATTERN, datas[index]);
//                    if (matches) {
                    try {
                        currentLong = Long.parseLong(datas[index]);//可能越界NumberFormatException
                        if (colType.equalsIgnoreCase("tinyint") && (currentLong > Byte.MAX_VALUE || currentLong < Byte.MIN_VALUE)) {
                            msg.setSuccess(false);
                            msg.setFalseInfo(String.format("第[%d]个字段越界,字段类型为[%s]", index, colType));
                            return msg;
                        } else if (colType.equalsIgnoreCase("smallint") && (currentLong > Short.MAX_VALUE || currentLong < Short.MIN_VALUE)) {
                            msg.setSuccess(false);
                            msg.setFalseInfo(String.format("第[%d]个字段越界,字段类型为[%s]", index, colType));
                            return msg;
                        } else if (colType.equalsIgnoreCase("int") && (currentLong > Integer.MAX_VALUE || currentLong < Integer.MIN_VALUE)) {
                            msg.setSuccess(false);
                            msg.setFalseInfo(String.format("第[%d]个字段越界,字段类型为[%s]", index, colType));
                            return msg;
                        }
                    } catch (NumberFormatException e) {
                        msg.setSuccess(false);
                        msg.setFalseInfo(String.format("第[%d]个字段越界,字段类型为[%s]", index, colType));
                        return msg;
                    }
//                    } else {
//                        msg.setSuccess(false);
//                        msg.setFalseInfo(String.format("第[%d]个字段格式错误,字段类型为[%s]", index, colType));
//                        return msg;
//                    }
                } else if (colType.equalsIgnoreCase("boolean")) {
                    if (!datas[index].equals("true") && !datas[index].equals("false")) {
                        msg.setSuccess(false);
                        msg.setFalseInfo(String.format("第[%d]boolean只支持true和false，大小写敏感", index));
                        return msg;
                    }
                } else if (colType.equalsIgnoreCase("date")) {
                    currentLong = convertDate(datas[index]);
                    if (currentLong == -1) {
                        msg.setSuccess(false);
                        msg.setFalseInfo(String.format("第[%d]个字段格式错误,字段类型为[%s]", index, colType));
                        return msg;
                    }
                } else if (colType.equalsIgnoreCase("char") || colType.equalsIgnoreCase("varchar")) {
                    byte[] buffer = datas[index].getBytes(StandardCharsets.UTF_8);
                    if (columnInfo.getBaseType() == FmdbDataType.VARCHAR && datas[index].length() > columnInfo.getPrecision()) {
                        msg.setSuccess(false);
                        msg.setFalseInfo(String.format("第[%d]个字段越界,精度为[%d],实际长度为[%d]", index, columnInfo.getPrecision(), buffer.length));
                        return msg;
                    }
                    if (columnInfo.getBaseType() == FmdbDataType.CHAR && datas[index].length() > columnInfo.getPrecision()) {
                        msg.setSuccess(false);
                        msg.setFalseInfo(String.format("第[%d]个字段长度错误,要求长度为[%d],实际长度为[%d]", index, columnInfo.getPrecision(), buffer.length));
                        return msg;
                    }
                } else if (colType.equalsIgnoreCase("float") || colType.equalsIgnoreCase("double")) {
//                    boolean matches = Pattern.matches(Constant.DOUBLE_PATTERN, datas[index]);
//                    if (matches) {
                    if (colType.equalsIgnoreCase("float")) {
                        try {
                            currentDouble = Float.parseFloat(datas[index]);//可能越界NumberFormatException
                        } catch (NumberFormatException e) {
                            msg.setSuccess(false);
                            msg.setFalseInfo(String.format("第[%d]个字段越界或为空串,字段类型为[%s]", index, colType));
                            return msg;
                        }
                    } else {
                        try {
                            currentDouble = Double.parseDouble(datas[index]);//可能越界NumberFormatException
                        } catch (NumberFormatException e) {
                            msg.setSuccess(false);
                            msg.setFalseInfo(String.format("第[%d]个字段越界或为空串,字段类型为[%s]", index, colType));
                            return msg;
                        }
                    }
//                    } else {//不是double类型的，格式错误
//                        msg.setSuccess(false);
//                        msg.setFalseInfo(String.format("第[%d]个字段格式错误,字段类型为[%s]", index, colType));
//                        return msg;
//                    }
                } else if (colType.equalsIgnoreCase("timestamp")) {
                    currentLong = CommonUtil.INSTANCE.convertTimesStamp(datas[index]);
                    if (currentLong == -1) {
                        msg.setSuccess(false);
                        msg.setFalseInfo(String.format("第[%d]个字段格式错误", index));
                        return msg;
                    }
                } else if (colType.equalsIgnoreCase("decimal")) {
                    try {
                        HiveDecimalWritable hiveDecimalWritable = new HiveDecimalWritable(datas[index]);
                        if (hiveDecimalWritable.getHiveDecimal().precision() > columnInfo.getPrecision()) {
                            msg.setSuccess(false);
                            msg.setFalseInfo(String.format("第[%d]个字段越界,precision设置的长度为[%d],传入的长度为[%d]", index, columnInfo.getPrecision(), hiveDecimalWritable.getHiveDecimal().precision()));
                            return msg;
                        }
                        if (hiveDecimalWritable.getScale() > columnInfo.getScale()) {
                            msg.setSuccess(false);
                            msg.setFalseInfo(String.format("第[%d]个字段越界,scale设置的长度为[%d],传入的长度为[%d]", index, columnInfo.getScale(), hiveDecimalWritable.getScale()));
                            return msg;
                        }
                    } catch (Exception e) {
                        msg.setSuccess(false);
                        msg.setFalseInfo(String.format("第[%d]个字段格式错误,数据类型要求为[%s]", index, columnInfo.getColType()));
                        return msg;
                    }
                } else if (colType.equalsIgnoreCase("string")) {
                    return msg;
                } else if (colType.equalsIgnoreCase(Constant.TEL)) {
                    boolean matches = Pattern.matches(Constant.TEL_PATTERN, datas[index]);
                    if (!matches) {
                        msg.setSuccess(false);
                        msg.setFalseInfo(String.format("第[%d]个字段格式错误,数据类型要求为[%s]", index, columnInfo.getColType()));
                        return msg;
                    }
                } else if (colType.equalsIgnoreCase(Constant.MAC)) {
                    System.out.println("todo------------------");
                }
            }
        }
        return msg;
    }

    /**
     * 将一行数据经过检验后加入到VectorizedRowBatch中
     *
     * @param datas              一行记录
     * @param vectorizedRowBatch rowbatch
     * @param tableInfo          表信息
     */
    public PutRecordMsg putRecord(String[] datas, VectorizedRowBatch vectorizedRowBatch, TableInfo tableInfo) {
        PutRecordMsg msg = new PutRecordMsg();
        int row = vectorizedRowBatch.size++;
        if (datas.length != tableInfo.getCols().size()) {
            row = vectorizedRowBatch.size--;
            logger.error("字段个数错误");
            msg.setSuccess(false);
            msg.setFalseInfo("字段个数错误");
            return msg;
        }
        //当前数值型的值得大小（orc都会转成long）
        long currentLong = 0;
        double currentDouble = 0;
        ColumnInfo columnInfo;
        String colType;
        int index = 0;// 字段下标
        for (ColumnVector columnVector : vectorizedRowBatch.cols) {
            columnInfo = tableInfo.getColumnInfo(index);
            colType = columnInfo.getBaseType().getDesc();
            if (datas[index] == null) {
                if (!columnInfo.isNull()) {
                    logger.error("第[{}]字段不能为空", index);
                    row = vectorizedRowBatch.size--;
                    msg.setSuccess(false);
                    msg.setFalseInfo(String.format("第[%d]字段不能为空", index));
                    return msg;
                } else {
                    columnVector.noNulls = false;
                    columnVector.isNull[row] = true;
                    index++;
                    continue;
                }
            }
            //以下字段的值不可能为null
            if (columnVector instanceof LongColumnVector) {
                if (colType.equalsIgnoreCase("tinyint") || colType.equalsIgnoreCase("smallint")
                        || colType.equalsIgnoreCase("int") || colType.equalsIgnoreCase("bigint")) {
//                    boolean matches = Pattern.matches(Constant.LONG_PATTERN, datas[index]);
//                    if (matches) {
                    try {
                        currentLong = Long.parseLong(datas[index]);//可能越界NumberFormatException
                        if (colType.equalsIgnoreCase("tinyint") && (currentLong > Byte.MAX_VALUE || currentLong < Byte.MIN_VALUE)) {
                            logger.error("第[{}]个字段越界,字段类型为[{}]", index, colType);
                            row = vectorizedRowBatch.size--;
                            msg.setSuccess(false);
                            msg.setFalseInfo(String.format("第[%d]个字段越界,字段类型为[%s]", index, colType));
                            return msg;
                        } else if (colType.equalsIgnoreCase("smallint") && (currentLong > Short.MAX_VALUE || currentLong < Short.MIN_VALUE)) {
                            logger.error("第[{}]个字段越界,字段类型为[{}]", index, colType);
                            row = vectorizedRowBatch.size--;
                            msg.setSuccess(false);
                            msg.setFalseInfo(String.format("第[%d]个字段越界,字段类型为[%s]", index, colType));
                            return msg;
                        } else if (colType.equalsIgnoreCase("int") && (currentLong > Integer.MAX_VALUE || currentLong < Integer.MIN_VALUE)) {
                            logger.error("第[{}]个字段越界,字段类型为[{}]", index, colType);
                            row = vectorizedRowBatch.size--;
                            msg.setSuccess(false);
                            msg.setFalseInfo(String.format("第[%d]个字段越界,字段类型为[%s]", index, colType));
                            return msg;
                        }
                    } catch (NumberFormatException e) {
                        logger.error("第[{}]个字段格式错误,字段类型为[{}]", index, colType);
                        row = vectorizedRowBatch.size--;
                        msg.setSuccess(false);
                        msg.setFalseInfo(String.format("第[%d]个字段越界,字段类型为[%s]", index, colType));
                        return msg;
                    }
//                    } else {
//                        logger.error("第[{}]个字段格式错误,字段类型为[{}]", index, colType);
//                        row = vectorizedRowBatch.size--;
//                        msg.setSuccess(false);
//                        msg.setFalseInfo(String.format("第[%d]个字段格式错误,字段类型为[%s]", index, colType));
//                        return msg;
//                    }
                } else if (colType.equalsIgnoreCase("boolean")) {
                    if (datas[index].equals("true")) {
                        currentLong = 1;
                    } else if (datas[index].equals("false")) {
                        currentLong = 0;
                    } else {
                        logger.error("第[{}]boolean只支持true和false，大小写敏感", index);
                        row = vectorizedRowBatch.size--;
                        msg.setSuccess(false);
                        msg.setFalseInfo(String.format("第[%d]boolean只支持true和false，大小写敏感", index));
                        return msg;
                    }
                } else if (colType.equalsIgnoreCase("date")) {
                    currentLong = convertDate(datas[index]);
                    if (currentLong == -1) {
                        logger.error("第[{}]个字段格式错误,字段类型为[{}]", index, colType);
                        row = vectorizedRowBatch.size--;
                        msg.setSuccess(false);
                        msg.setFalseInfo(String.format("第[%d]个字段格式错误,字段类型为[%s]", index, colType));
                        return msg;
                    }
                }
                ((LongColumnVector) columnVector).vector[row] = currentLong;
            } else if (columnVector instanceof BytesColumnVector) {
                byte[] buffer = datas[index].getBytes(StandardCharsets.UTF_8);
                if (columnInfo.getBaseType() == FmdbDataType.VARCHAR && datas[index].length() > columnInfo.getPrecision()) {
                    logger.error("第[{}]个字段越界,精度为[{}],实际长度为[{}]", index, columnInfo.getPrecision(), buffer.length);
                    row = vectorizedRowBatch.size--;
                    msg.setSuccess(false);
                    msg.setFalseInfo(String.format("第[%d]个字段越界,精度为[%d],实际长度为[%d]", index, columnInfo.getPrecision(), buffer.length));
                    return msg;
                }
                if (columnInfo.getBaseType() == FmdbDataType.CHAR && datas[index].length() > columnInfo.getPrecision()) {
                    logger.error("第[{}]个字段长度错误,要求长度为[{}],实际长度为[{}]", index, columnInfo.getPrecision(), buffer.length);
                    row = vectorizedRowBatch.size--;
                    msg.setSuccess(false);
                    msg.setFalseInfo(String.format("第[%d]个字段长度错误,要求长度为[%d],实际长度为[%d]", index, columnInfo.getPrecision(), buffer.length));
                    return msg;
                }
                if (columnInfo.getColType().equalsIgnoreCase(Constant.TEL) && !Pattern.matches(Constant.TEL_PATTERN, datas[index])) {
                    row = vectorizedRowBatch.size--;
                    msg.setSuccess(false);
                    msg.setFalseInfo(String.format("第[%d]个字段格式错误,数据类型要求为[%s]", index, columnInfo.getColType()));
                    return msg;
                }

                ((BytesColumnVector) columnVector).setRef(row, buffer, 0, buffer.length);
            } else if (columnVector instanceof DoubleColumnVector) {
//                boolean matches = Pattern.matches(Constant.DOUBLE_PATTERN, datas[index]);
//                if (matches) {
                try {
                    currentDouble = Double.parseDouble(datas[index]);//可能越界NumberFormatException
                } catch (NumberFormatException e) {
                    logger.error("第[{}]个字段越界或为空串,字段类型为[{}]", index, colType);
                    row = vectorizedRowBatch.size--;
                    msg.setSuccess(false);
                    msg.setFalseInfo(String.format("第[%d]个字段越界或为空串,字段类型为[%s]", index, colType));
                    return msg;
                }
                if (colType.equalsIgnoreCase("float")) {
                    try {
                        currentDouble = Float.parseFloat(datas[index]);
                    } catch (NumberFormatException e) {
                        logger.error("第[{}]个字段格式错误,字段类型为[{}]", index, colType);
                        row = vectorizedRowBatch.size--;
                        msg.setSuccess(false);
                        msg.setFalseInfo(String.format("第[%d]个字段越界,字段类型为[%s]", index, colType));
                        return msg;
                    }
                }
                ((DoubleColumnVector) columnVector).vector[row] = currentDouble;
                //目前还不支持float
//                } else {//不是double类型的，格式错误
//                    logger.error("第[{}]个字段格式错误,字段类型为[{}]", index, colType);
//                    row = vectorizedRowBatch.size--;
//                    msg.setSuccess(false);
//                    msg.setFalseInfo(String.format("第[%d]个字段格式错误,字段类型为[%s]", index, colType));
//                    return msg;
//                }
            } else if (columnVector instanceof TimestampColumnVector) {
                currentLong = CommonUtil.INSTANCE.convertTimesStamp(datas[index]);
                if (currentLong == -1) {
                    logger.error("第[{}]个字段格式错误", index);
                    row = vectorizedRowBatch.size--;
                    msg.setSuccess(false);
                    msg.setFalseInfo(String.format("第[%d]个字段格式错误", index));
                    return msg;
                }
                ((TimestampColumnVector) columnVector).time[row] = currentLong;
            } else if (columnVector instanceof DecimalColumnVector) {
                try {
                    HiveDecimalWritable hiveDecimalWritable = new HiveDecimalWritable(datas[index]);
                    if (hiveDecimalWritable.getHiveDecimal().precision() > columnInfo.getPrecision()) {
                        logger.error("第[{}]个字段越界,precision设置的长度为[{}],传入的长度为[{}]", index, columnInfo.getPrecision(), hiveDecimalWritable.getHiveDecimal().precision());
                        row = vectorizedRowBatch.size--;
                        msg.setSuccess(false);
                        msg.setFalseInfo(String.format("第[%d]个字段越界,precision设置的长度为[%d],传入的长度为[%d]", index, columnInfo.getPrecision(), hiveDecimalWritable.getHiveDecimal().precision()));
                        return msg;
                    }
                    if (hiveDecimalWritable.getScale() > columnInfo.getScale()) {
                        logger.error("第[{}]个字段越界,scale设置的长度为[{}],传入的长度为[{}]", index, columnInfo.getScale(), hiveDecimalWritable.getScale());
                        row = vectorizedRowBatch.size--;
                        msg.setSuccess(false);
                        msg.setFalseInfo(String.format("第[%d]个字段越界,scale设置的长度为[%d],传入的长度为[%d]", index, columnInfo.getScale(), hiveDecimalWritable.getScale()));
                        return msg;
                    }
                    ((DecimalColumnVector) columnVector).vector[row] = new HiveDecimalWritable(datas[index]);
                } catch (Exception e) {
                    logger.error("第[{}]个字段格式错误,数据类型要求为[{}]", index, columnInfo.getBaseType().getDesc());
                    row = vectorizedRowBatch.size--;
                    msg.setSuccess(false);
                    msg.setFalseInfo(String.format("第[%d]个字段格式错误,数据类型要求为[%s]", index, columnInfo.getBaseType().getDesc()));
                    return msg;
                }
            } else {
                throw new RuntimeException("不支持" + columnVector);
            }
            index++;
        }
        return msg;
    }

    /**
     * 根据表名，分区类型，ttl获取所有的分区表名
     *
     * @param partitionType 分区类型
     * @param ttl           ttl
     * @return 所有的分区表名
     */
    public Set<String> getPartNames(PartitionType partitionType, int ttl) {
        Set<String> result = Sets.newHashSet();
        Calendar calendar = null;
        SimpleDateFormat sdf;
        switch (partitionType) {
            case DAY:
                sdf = new SimpleDateFormat("yyyyMMdd");
                for (int i = 0; i < ttl; i++) {
                    calendar = Calendar.getInstance();
                    calendar.add(Calendar.DAY_OF_YEAR, -i);
                    result.add(sdf.format(calendar.getTime()));
                }
                break;
            case MONTH:
                sdf = new SimpleDateFormat("yyyyMM");
                for (int i = 0; i < ttl; i++) {
                    calendar = Calendar.getInstance();
                    calendar.add(Calendar.MONTH, -i);
                    result.add(sdf.format(calendar.getTime()));
                }
                break;
            case YEAR:
                sdf = new SimpleDateFormat("yyyy");
                for (int i = 0; i < ttl; i++) {
                    calendar = Calendar.getInstance();
                    calendar.add(Calendar.YEAR, -i);
                    result.add(sdf.format(calendar.getTime()));
                }
                break;
        }
        return result;
    }

    /**
     * 将时间转换成orc格式的date，1970-1-1 --> 0/1970-1-2 --> 1
     *
     * @param date 日期
     * @return orc格式的date
     */
    public long convertDate(String date) {
        LocalDate start = LocalDate.of(1970, 1, 1);
        String[] split = date.split(Constant.LINETHROUGH);
        if (split.length != 3) {
            logger.error("[{}]传入格式错误", date);
            return -1;
        }
        int year = 0;
        int month = 0;
        int day = 0;
        try {
            year = Integer.parseInt(split[0]);
            month = Integer.parseInt(split[1]);
            day = Integer.parseInt(split[2]);
        } catch (NumberFormatException e) {
            logger.error("[{}]传入格式错误", date, e);
            return -1;
        }
        LocalDate end = LocalDate.of(year, month, day);
        return end.toEpochDay() - start.toEpochDay();
    }

    public long convertTimesStamp(String capturetime) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        Date parse = null;
        try {
            parse = sdf.parse(capturetime);
        } catch (ParseException e) {
            logger.error("[{}]格式错误", capturetime, e);
            return -1;
        }
        return parse.getTime();
    }

    /**
     * 根据分区字段获得分区名
     *
     * @param content
     * @param partitionType
     * @param ttl
     * @return
     */
    public String getPartName(String content, PartitionType partitionType, int ttl) {
        Calendar calendar = Calendar.getInstance();
        SimpleDateFormat sdf = null;
        Date date = new Date();
        try {
            date.setTime(Long.parseLong(content) * 1000);
        } catch (NumberFormatException e) {
            logger.error("分区字段格式错误", e);
            return null;
        }
        switch (partitionType) {
            case DAY:
                if (ttl != -1) {
                    calendar.set(Calendar.HOUR_OF_DAY, 0);
                    calendar.set(Calendar.MINUTE, 0);
                    calendar.set(Calendar.SECOND, 0);
                    calendar.set(Calendar.MILLISECOND, 0);
                    calendar.add(Calendar.DAY_OF_YEAR, -ttl + 1);
                    if (Long.parseLong(content) < calendar.getTime().getTime() / 1000 || Long.parseLong(content) > System.currentTimeMillis() / 1000) {
                        logger.error("分区字段[{}]超时", content);
                        return null;
                    }
                }
                sdf = new SimpleDateFormat("yyyyMMdd");
                break;
            case MONTH:
                if (ttl != -1) {
                    calendar.set(Calendar.DAY_OF_MONTH, 1);
                    calendar.set(Calendar.HOUR_OF_DAY, 0);
                    calendar.set(Calendar.MINUTE, 0);
                    calendar.set(Calendar.SECOND, 0);
                    calendar.set(Calendar.MILLISECOND, 0);
                    calendar.add(Calendar.MONTH, -ttl + 1);
                    if (Long.parseLong(content) < calendar.getTime().getTime() / 1000 || Long.parseLong(content) > System.currentTimeMillis() / 1000) {
                        logger.error("分区字段[{}]超时", content);
                        return null;
                    }
                }
                sdf = new SimpleDateFormat("yyyyMM");
                break;
            case YEAR:
                if (ttl != -1) {
                    calendar.set(Calendar.MONTH, 0);
                    calendar.set(Calendar.DAY_OF_MONTH, 1);
                    calendar.set(Calendar.HOUR_OF_DAY, 0);
                    calendar.set(Calendar.MINUTE, 0);
                    calendar.set(Calendar.SECOND, 0);
                    calendar.set(Calendar.MILLISECOND, 0);
                    calendar.add(Calendar.YEAR, -ttl + 1);
                    sdf = new SimpleDateFormat("yyyy");
                    if (Long.parseLong(content) < calendar.getTime().getTime() / 1000 || Long.parseLong(content) > System.currentTimeMillis() / 1000) {
                        logger.error("分区字段[{}]超时", content);
                        return null;
                    }
                }
                sdf = new SimpleDateFormat("yyyy");
                break;
        }
        return sdf.format(date);
    }
}
