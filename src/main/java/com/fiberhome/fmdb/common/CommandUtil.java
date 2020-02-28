package com.fiberhome.fmdb.common;
/**
 * @author sjj
 * @version 2019年9月30日下午4:24:18
 */

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

public class CommandUtil {


    public static Options getStatisticsOptions() {
        Options options = new Options();

        Option helpOption = new Option("h", "help", false, "Print the usage information");
        options.addOption(helpOption);

        Option tableOption = new Option("f", "fileDir", true, "orc dir location(必填)");
        tableOption.setArgs(1);
        tableOption.setArgName("fileDir");
        options.addOption(tableOption);

        Option tsOption = new Option("ts", "tbaleSize", false, "是否展示表占空间大小");
        options.addOption(tsOption);

        Option csOption = new Option("cs", "columnSize", false, "是否展示每列占的空间大小");
        options.addOption(csOption);

        Option mmOption = new Option("mm", "min_max", false, "是否展示每个文件中每个列的最小值最大值");
        options.addOption(mmOption);

        Option dnOption = new Option("dn", "dataNum", false, "是否展示每个文件中的数据量,以及表的总数据量");
        options.addOption(dnOption);

        return options;
    }

    /**
     * 获取入库工具的导航参数
     *
     * @return
     */
    public static Options getImportDataOptions() {
        Options options = new Options();

        Option helpOption = new Option("h", "help", false, "Print the usage information");
        options.addOption(helpOption);

        Option dbOption = new Option("db", "dbName", true, "import which db");
        dbOption.setArgs(1);
        dbOption.setArgName("dbName");
        options.addOption(dbOption);

        Option tableOption = new Option("t", "table", true, "import which table");
        tableOption.setArgs(1);
        tableOption.setArgName("tableNmae");
        options.addOption(tableOption);

        Option fileOption = new Option("f", "file", true, "filePath location");
        fileOption.setArgs(1);
        fileOption.setArgName("filePath");
        options.addOption(fileOption);

        Option overOption = new Option("o", "over", true, "overwriter the old data or not");
        overOption.setArgs(1);
        overOption.setArgName("overwriter");
        options.addOption(overOption);
        return options;

    }
}
