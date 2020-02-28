package com.fiberhome.fmdb.manifest.colcompare;

import com.fiberhome.fmdb.manifest.bean.DataRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 处理double类型的
 */
@Deprecated
public class DoubleRangeCompare implements IDataRangeCompare{
    private static Logger logger = LoggerFactory.getLogger(DoubleRangeCompare.class);

    @Override
    public boolean compare(DataRange dataRange, String start, String end) {
        if (start == null && end == null) {
            return true;
        }
        //orc元数据中的最小值
        double metaMin = Double.parseDouble(dataRange.getMin());
        //orc元数据中的最大值
        double metaMax = Double.parseDouble(dataRange.getMax());

        double min = Double.MIN_VALUE;
        double max = Double.MAX_VALUE;
        if (start != null) {
            min = Double.parseDouble(start);
        }
        if (end != null) {
            max = Double.parseDouble(end);
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
