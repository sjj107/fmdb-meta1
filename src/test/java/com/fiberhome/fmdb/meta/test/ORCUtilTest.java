package com.fiberhome.fmdb.meta.test;

import com.fiberhome.fmdb.manifest.bean.OrcInfo;
import com.fiberhome.fmdb.statistic.ORCUtil;

/**
 * @Description orc工具测试类
 * @Author sjj
 * @Date 19/11/01 下午 04:42
 **/
public class ORCUtilTest {
    public static void main(String[] args) {
        OrcInfo orcinfo = ORCUtil.INSTANCE.getOrcinfo("", "", "", "", "");
        System.out.println(orcinfo.getColInfoMap().size());
    }
}
