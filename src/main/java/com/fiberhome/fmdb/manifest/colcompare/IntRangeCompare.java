package com.fiberhome.fmdb.manifest.colcompare;

import com.fiberhome.fmdb.manifest.bean.DataRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
@Deprecated
public class IntRangeCompare implements IDataRangeCompare {
    private static Logger logger = LoggerFactory.getLogger(IntRangeCompare.class);

    @Override
    public boolean compare(DataRange dataRange, String start, String end) {
        if (start == null && end == null) {
            return true;
        }
        //orc元数据中的最小值
        int metaMin = Integer.parseInt(dataRange.getMin());
        //orc元数据中的最大值
        int metaMax = Integer.parseInt(dataRange.getMax());

        int min = Integer.MIN_VALUE;
        int max = Integer.MAX_VALUE;
        if (start != null) {
            min = Integer.parseInt(start);
        }
        if (end != null) {
            max = Integer.parseInt(end);
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
