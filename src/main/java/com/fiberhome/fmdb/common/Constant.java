package com.fiberhome.fmdb.common;

import java.io.File;
import java.io.FileFilter;

/**
 * Created by sjj on 19/10/17
 */
public class Constant {
    public static final String FMDB_META_LOG_NAME = "fmdbmeta";
    /**
     * orc元数据为文件名
     */
    public static final String ORC_META_FILE_NAME = "manifest";

    /**
     * ORC文件前缀名
     */
    public static final String ORC_FILE_NAME_PREFIX = "DATA_";

    /**
     * ORC临时文件后缀名
     */
    public static final String ORC_TMP_FILE_NAME_SUFFIX = ".orc";
    /**
     * ORC文件后缀名
     */
    public static final String ORC_FILE_NAME_SUFFIX = ".orc";

    /**
     * linux系统上，json文件默认存放的位置
     */
    public static String DEFAULT_JSON_DIR_LINUX = "/home/nebula/fmdb/json";
    /**
     * window系统上，json文件默认存放的位置
     */
    public static String DEFAULT_JSON__DIR_WIN = "E:\\data\\fmdb\\json";
    /**
     * linux上数据默认存放位置
     */
    public static String DEFAULT_DATA_DIR_LINUX = "/data/orcfile";
    /**
     * windows上数据默认存放位置
     */
    public static String DEFAULT_DATA_DIR_WIN = "E:\\data\\fmdb\\data";

    /**
     * 数据存放的目录，与wal区分
     */
    public static String DEFAULT_DATA_BASE = "base";
    /**
     * 元数据默认存放位置
     */
    public static String DEFAULT_META_LOCATION = "PG";
    /**
     * 默认更新manifest的周期
     */
    public static String DEFAULT_MANIFEST_CROND = "0 0/1 * * * ? *";
    /**
     * 默认加载manifest的周期
     */
    public static String DEFAULT_MANIFESTCACHE_CROND = "0 0/1 * * * ? *";

    /**
     * 斜杠
     */
    public static final String SLASH = "/";
    /**
     * 下划线
     */
    public static final String UNDERLINE = "_";
    /**
     * 中划线
     */
    public static final String LINETHROUGH = "-";
    /**
     * TAB键
     */
    public static final String TAB = "\t";

    /**
     * 左括号
     */
    public static final String LEFT_BRACKET = "(";
    /**
     * 左括号
     */
    public static final String RIGHT_BRACKET = ")";

    /**
     * 逗号
     */
    public static final String COMMA = ",";
    /**
     * 冒号
     */
    public static final String COLON = ":";

    /**
     * 点
     */
    public static final String DOT = ".";

    /**
     * 库名可表名的连接符
     */
    public static final String DB_TBL_CON = "::";

    //double类型的正则表达式
    public static final String DOUBLE_PATTERN = "[-|+]?\\d+\\.?\\d*(d|D)?[f|F]?";
    //double类型的正则表达式
    public static final String LONG_PATTERN = "[-|+]?\\d+(l|L)?";
    //orc文件变化监控时间间隔（单位毫秒）
    public static final int MONITOR_INTERVAL = 3000;
    /**
     * ORC文件过滤
     */
    public static final FileFilter FILE_FILTER = new FileFilter() {
        @Override
        public boolean accept(File pathname) {
            String name = pathname.getName();
            return !name.startsWith(UNDERLINE) && !name.startsWith(DOT) && !name.contains(ORC_META_FILE_NAME);
        }
    };

    public static final FileFilter ORC_TMP_FILE = new FileFilter() {
        @Override
        public boolean accept(File pathname) {
            String name = pathname.getName();
            return !name.startsWith(UNDERLINE) && !name.startsWith(DOT) && !name.contains(ORC_META_FILE_NAME) && name.endsWith(ORC_TMP_FILE_NAME_SUFFIX);
        }
    };

    /**
     * 配置文件路径
     */
    public static final String FMDB_CONF_PATH = "conf/fmdb_meta.properties";
    /**
     * log4j文件路径
     */
    public static final String LOG4J_CONF_PATH = "conf/log4j.properties";
    /**
     * 数据存放位置配置项
     */
    public static final String FMDB_DATA_DIR = "dataDir";
    /**
     * FMDB元数据存放位置
     */
    public static final String FMDB_MEAT_LOCATION = "metaLocation";
    /**
     * 元数据存放位置配置项
     */
    public static final String FMDB_JSON_DIR = "jsonDir";
    /**
     * 表名后缀
     */
    public static final String TABLE_SUFFIX = ".tbl";
    /**
     * 表名后缀
     */
    public static final String INDEX_SUFFIX = ".idx";

    /**
     * 全量表的后缀名
     */
    public static final String TOTAL_SUFFIX = "all";
    /**
     * 分区字段名
     */
    public static final String PARTITION_NAME = "partition.column";
    /**
     * 分区字段类型
     */
    public static final String PARTITION_TYPE = "partition.type";
    /**
     * 排序字段列表
     */
    public static final String SORT_FIELDS = "order-by.columns";
    /**
     * 排序类型，升序或降序
     */
    public static final String SORT_TYPE = "order-by.type";
    /**
     * 表的存储周期
     */
    public static final String TTL = "data.ttl";
    /**
     * 每个orc文件的大小
     */
    public static final String ORC_SIZE = "storage.file.size";
    /**
     * 表的压缩方式
     */
    public static final String COMPRESSTYPE = "data.compress.type";
    /**
     * 索引文件过滤
     */
    public static final FileFilter INDEX_FILTER = new FileFilter() {
        @Override
        public boolean accept(File pathname) {
            String name = pathname.getName();
            return name.endsWith(INDEX_SUFFIX);
        }
    };

    /**
     * 离线入库批量数据大小
     */
    public static int FMDBBULKLOAD_BATCHSIZE=1000000;

    //H2相关
    //H2驱动
    public static final String H2_DRIVER_CLASS = "org.h2.Driver";
    //H2 url前缀
    public static final String H2_JDBC_URL_PRE = "jdbc:h2:tcp://";
    //H2ip
    //端口号
    public static int H2_PORT = 7770;
    //H2 ip地址
    public static String H2_IP = "localhost";
    //H2数据库数据默认存放位置
    public static String H2_DATA_DIR = "/data/H2/DB";

    //PG相关
    //PG驱动
    public static final String PG_DRIVER_CLASS = "org.postgresql.Driver";
    //PG url前缀
    public static final String PG_JDBC_URL_PRE = "jdbc:postgresql://";
    //端口号
    public static int PG_PORT = 5432;
    //PG ip地址
    public static String PG_IP = "localhost";
    //PG数据库
    public static String PG_DATABASE = "/postgres";
    //用户名
    public static String PG_USER = "postgres";
    //密码
    public static String PG_PSW = "postgres";
    //ZK连接串
    public static String ZK_URL = "localhost";
    //第三方依赖的包
    public static String EXTERNAL_JARS_DIR = "./external-lib";
    //自orc文件创建后，隔多久必须关闭，单位：min
    public static int ORC_FLUSH_INTERVAL = 7;

    //连接池相关
    //连接池初始化大小
    public static String DS_INITIALSIZE = "10";
    //连接池最大数量
    public static String DS_MAXACTIVE = "20";
    //连接池最大等待时间
    public static String DS_MAXWAIT = "2000";
    //连接池最小空闲
    public static String DS_MINIDLE = "10";
    //用户自定义字段类型的key
    public static String UDCT_KEY = "customized_type_name";

    public static String DELETE_TBL_ZK = "/FMDB/deleted/table/";//删表在zk上记录
    public static String DELETE_DB_ZK = "/FMDB/deleted/db/";//删库在zk上记录
    public static String TRUNCATE_TBL_ZK = "/FMDB/truncate/table/";//清表在zk上记录

    //自定义数据类型相关
    //Configuration的可以值
    public static String CONF_KEY = "orc.custom.encode.impl";
    //手机号名称
    public static final String TEL = "tel";
    //手机号正则
    public static final String TEL_PATTERN = "(\\b)(1[345789][0-9]{9})(\\b)";
    //手机号名称
    public static String MAC = "mac";
    //手机号正则
    public static final int MAC_LEN = 12;


}
