package com.fiberhome.fmdb.meta.bean;

/**
 * @Description TODO
 * @Author sjj
 * @Date 2020/2/28 22:12
 */
public class Category {
    private final boolean isPrimitive;
    private final String name;

    public Category(boolean isPrimitive, String name) {
        this.isPrimitive = isPrimitive;
        this.name = name;
    }

    public boolean isPrimitive() {
        return isPrimitive;
    }

    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return name.toLowerCase();
    }
}
