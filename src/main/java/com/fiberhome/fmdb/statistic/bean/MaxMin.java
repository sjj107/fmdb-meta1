package com.fiberhome.fmdb.statistic.bean;

public class MaxMin {
    private Object max;
    private Object min;

    public MaxMin(Object max, Object min) {
        this.max = max;
        this.min = min;
    }

    @Override
    public String toString() {
        return "[" + min + ", " + max + "]";
    }
}
