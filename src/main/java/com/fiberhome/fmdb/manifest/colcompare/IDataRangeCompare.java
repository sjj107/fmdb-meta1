package com.fiberhome.fmdb.manifest.colcompare;


import com.fiberhome.fmdb.manifest.bean.DataRange;

/**
 * 定义比较数据是否有交集
 */
@Deprecated
public interface IDataRangeCompare {
    /**
     * 比较数据是否有交集
     *
     * @param dataRange 比较对象
     * @param start     最小值
     * @param end       最大值
     * @return
     */
    boolean compare(DataRange dataRange, String start, String end);
}
