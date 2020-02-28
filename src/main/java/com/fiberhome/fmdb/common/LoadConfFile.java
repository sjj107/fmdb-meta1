package com.fiberhome.fmdb.common;

import java.io.*;
import java.util.Properties;

/**
 * 加载配置文件
 */
public class LoadConfFile {
//    public static void loadLog4j(String confPath) {
//        Properties sysProperties = System.getProperties();
//        String sysName = sysProperties.getProperty("os.name");
//        String sysPath = "";
//        if (!sysName.contains("Windows")) {
//            sysPath = "../";
//        }
//        confPath = sysPath + confPath;
//        PropertyConfigurator.configure(confPath);
//    }


    public static Properties load(String confPath) throws IOException {
        Properties sysProperties = System.getProperties();
        String sysName = sysProperties.getProperty("os.name");
        String sysPath = "../";
/*        if (!sysName.contains("Windows")) {
            sysPath = "../";
        }*/
//        confPath = sysPath + confPath;
        InputStream inputStream = null;
        try {
            inputStream = new FileInputStream(new File(confPath));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        Properties properties = new Properties();
        try {
            properties.load(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (inputStream != null) {
                inputStream.close();
            }
        }
        return properties;
    }
}
