package com.fiberhome.fmdb.manifest.colcompare;

import com.fiberhome.fmdb.manifest.bean.DataRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 处理bigint类型的
 */
@Deprecated
public class BigIntRangeCompare implements IDataRangeCompare {
    private static Logger logger = LoggerFactory.getLogger(BigIntRangeCompare.class);

    @Override
    public boolean compare(DataRange dataRange, String start, String end) {
        if (start == null && end == null) {
            return true;
        }
        //orc元数据中的最小值
        long metaMin = Long.parseLong(dataRange.getMin());
        //orc元数据中的最大值
        long metaMax = Long.parseLong(dataRange.getMax());

        long min = Long.MIN_VALUE;
        long max = Long.MAX_VALUE;
        if (start != null) {
            min = Long.parseLong(start);
        }
        if (end != null) {
            max = Long.parseLong(end);
        }
        if (min > max) {
            logger.error("最小值[{}]大于最大值[{}]", start, end);
            throw new IllegalArgumentException("最小值大于最大值");
        }
        if (max < metaMin) {
            return false;
        }
        if (min > metaMax) {
            return false;
        }
        return true;
    }
}
