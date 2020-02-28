package com.fiberhome.fmdb.manifest.colcompare;

/**
 * DataRange比较工厂类
 */
@Deprecated
public class DataRangeCompareFactory {
    /**
     * @param colType 字段类型
     * @return 比较类
     */
    public static IDataRangeCompare getCompare(String colType) {
        if (colType == null) {
            return null;
        } else if (colType.equalsIgnoreCase("int")) {
            return new IntRangeCompare();
        } else if (colType.equalsIgnoreCase("bigint")) {
            return new BigIntRangeCompare();
        } else if (colType.equalsIgnoreCase("double")) {
            return new DoubleRangeCompare();
        } else if (colType.equalsIgnoreCase("string")) {
            return new StringRangeCompare();
        } else {
            throw new IllegalArgumentException("不支持类型：" + colType);
        }
    }
}
