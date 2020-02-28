package com.fiberhome.fmdb.meta.test;

import com.fiberhome.fmdb.common.CommonUtil;
import com.fiberhome.fmdb.common.Constant;
import com.fiberhome.fmdb.meta.bean.PartitionType;

import java.io.File;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * @Description 通用工具测试类
 * @Author sjj
 * @Date 19/11/04 上午 11:16
 **/
public class CommonUtilTest {
    public static void main(String[] args) {
//        File orcTmpFile = new File("E:\\data\\orc\\data0.1.0\\fborc\\datatype\\DATA_0.tmp");
//        boolean b = CommonUtil.INSTANCE.reNameOrcTmpFile(orcTmpFile);
//        System.out.println(b);

//        Set<String> http = CommonUtil.INSTANCE.getPartNames(PartitionType.YEAR, 3);
//        long l = CommonUtil.INSTANCE.convertDate("2019-11-14");

        boolean matches =Pattern.matches(Constant.TEL_PATTERN, "173687044996");
        System.out.println(matches);
    }
}
