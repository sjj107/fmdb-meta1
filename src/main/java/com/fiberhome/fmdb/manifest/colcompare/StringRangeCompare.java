package com.fiberhome.fmdb.manifest.colcompare;

import com.fiberhome.fmdb.manifest.bean.DataRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 处理字符串类型的
 */
@Deprecated
public class StringRangeCompare implements IDataRangeCompare {
    private static Logger logger = LoggerFactory.getLogger(StringRangeCompare.class);

    @Override
    public boolean compare(DataRange dataRange, String start, String end) {
        String metaMin = dataRange.getMin();
        String metaMax = dataRange.getMax();
        if (start == null && end == null) {
            return true;
        }
        if (start.compareTo(end) > 0) {
            logger.error("最小值[{}]大于最大值[{}]", start, end);
            throw new IllegalArgumentException("最小值大于最大值");
        }
        if (start != null && end != null) {
            if (end.compareTo(metaMin) < 0) {
                return false;
            }
            if (start.compareTo(metaMax) > 0) {
                return false;
            }
        } else if (start == null) {
            if (end.compareTo(metaMin) < 0) {
                return false;
            }
        } else {
            if (start.compareTo(metaMax) > 0) {
                return false;
            }
        }
        return true;
    }
}
