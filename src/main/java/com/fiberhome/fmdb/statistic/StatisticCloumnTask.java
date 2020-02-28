package com.fiberhome.fmdb.statistic;

import com.fiberhome.fmdb.common.Constant;
import com.fiberhome.fmdb.meta.bean.StaticColumn;
import com.fiberhome.fmdb.meta.conf.FMDBMetaConf;
import com.fiberhome.fmdb.meta.factory.Factory;
import com.fiberhome.fmdb.meta.factory.FmdbMetaFactory;
import com.fiberhome.fmdb.meta.factory.PGFactory;
import com.fiberhome.fmdb.meta.tool.IFMDBMetaClient;
import com.fiberhome.fmdb.meta.tool.impl.PGClient;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.orc.ColumnStatistics;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.OrcAcidUtils;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * 定期统计每列的信息
 */
public class StatisticCloumnTask implements Runnable {

    public static final PathFilter HIDDEN_AND_SIDE_FILE_FILTER = new PathFilter() {
        public boolean accept(Path p) {
            String name = p.getName();
            return !name.startsWith("_") && !name.startsWith(".") && !name.endsWith(
                    OrcAcidUtils.DELTA_SIDE_FILE_SUFFIX) && !name.contains(Constant.ORC_META_FILE_NAME);
        }
    };

    // 元数据客户端
    public static IFMDBMetaClient metaClient;

    //数据基础目录   $BASEDIR/db/tableName/
    public static String dataDir;

    public static PGClient pgClient;

    public StatisticCloumnTask() {
        init();
    }

    private void init() {
        FMDBMetaConf instance = FMDBMetaConf.getInstance();

        String pg_ip = instance.pg_ip;
        int pg_port = instance.pg_port;
        String pg_user = instance.pg_user;
        String pg_psw = instance.pg_psw;
        String pg_dataBase = instance.pg_dataBase;
        pgClient = PGClient.getInstance(pg_ip, pg_port, pg_user, pg_psw, pg_dataBase);

        Factory pgFactory = new PGFactory();
        metaClient = FmdbMetaFactory.INSTANCE.getMetaClient();
        dataDir = instance.dataDir;

    }

    @Override
    public void run() {
        // 清空之前的记录信息
        pgClient.truncateTableCloumnSize();

        // <db,tableName...>
        ArrayListMultimap<String, String> multimap = ArrayListMultimap.create();
        // 获取每张库表的信息表名
        Set<String> allDBNames = metaClient.getAllDBNames();

        Iterator<String> iterator = allDBNames.iterator();

        ArrayList<StaticColumn> staticColumns = Lists.newArrayList();

        while (iterator.hasNext()) {
            String dbName = iterator.next();
            List<String> allTableName = metaClient.getAllTableName(dbName);
            for (int i = 0; i < allTableName.size(); i++) {
                multimap.put(dbName, allTableName.get(i));

                try {
                    Map<String, Long> colSize = showStatistics(dataDir.toString() + "/" + dbName + "/" + allTableName.get(i));

                    Set<Map.Entry<String, Long>> entries = colSize.entrySet();

                    for (Map.Entry<String, Long> entry : entries) {
                        staticColumns.add(new StaticColumn(dbName, allTableName.get(i), entry.getKey(), entry.getValue()));
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    continue;
                }
            }
        }

        pgClient.storageCloumnSize(staticColumns);
    }


    private Map<String, Long> showStatistics(String tableDir) throws IOException {
        //每列占的空间大小<字段名，大小>
        Map<String, Long> colSize = Maps.newHashMap();

        Configuration config = new Configuration();

        //当前表的路径
        Path path = new Path(tableDir);
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
//            tableTotalSize.setOrigSize(tableTotalSize.getOrigSize() + len);
            //统计每个列的占空间大小
            reader = OrcFile.createReader(new Path(filename), OrcFile.readerOptions(config));
            //每个orc文件的数量
            long numberOfRows = reader.getNumberOfRows();
//            fileNums.put(filename, numberOfRows);
            TypeDescription schema = reader.getSchema();
            fieldNames = schema.getFieldNames();
            ColumnStatistics[] stats = reader.getStatistics();

            for (int i = 1; i < stats.length; ++i) {
                ColumnStatistics statistics = stats[i];
                long bytesOnDisk = statistics.getBytesOnDisk();
                if (colSize.get(fieldNames.get(i - 1)) != null) {
                    colSize.put(fieldNames.get(i - 1), colSize.get(fieldNames.get(i - 1)) + bytesOnDisk);
                } else {
                    colSize.put(fieldNames.get(i - 1), bytesOnDisk);
                }
            }

        }
        return colSize;
    }


    private static Collection<String> getAllFilesInPath(final Path path,
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
