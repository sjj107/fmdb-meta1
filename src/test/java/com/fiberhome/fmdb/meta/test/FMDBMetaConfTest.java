package com.fiberhome.fmdb.meta.test;

import com.fiberhome.fmdb.common.Constant;
import com.fiberhome.fmdb.common.LoadConfFile;
import com.fiberhome.fmdb.meta.conf.FMDBMetaConf;

/**
 * @Description 配置文件的测试类
 * @Author sjj
 * @Date 19/11/02 下午 03:33
 **/
public class FMDBMetaConfTest {
    public static void main(String[] args) {
//        LoadConfFile.loadLog4j(Constant.LOG4J_CONF_PATH);
        FMDBMetaConf instance = FMDBMetaConf.getInstance();
        System.out.println(instance.FMDBDataDir);
        System.out.println(instance.dataDir);

    }
}
