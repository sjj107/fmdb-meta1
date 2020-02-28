package com.fiberhome.fmdb.manifest.bean;

import java.io.Serializable;

/**
 * 用于描述数据的范围
 */
public class DataRange implements Serializable {
    private static final long serialVersionUID = -7251299239132353505L;
    /**
     * 最小值
     */
    private String min;
    /**
     * 最大值
     */
    private String max;

    public DataRange(String min, String max) {
        this.min = min;
        this.max = max;
        if(min==null || max==null){
           throw new IllegalArgumentException("最小和最大值不能同时为空");
        }
    }

    public String getMin() {
        return min;
    }

    public String getMax() {
        return max;
    }

    @Override
    public String toString() {
        return "DataRange{" +
                "min=" + min +
                ", max=" + max +
                '}';
    }
}
