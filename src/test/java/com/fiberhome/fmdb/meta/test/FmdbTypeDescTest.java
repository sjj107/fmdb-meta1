package com.fiberhome.fmdb.meta.test;

import com.fiberhome.fmdb.meta.bean.FmdbTypeDesc;

/**
 * @Description TODO
 * @Author sjj
 * @Date 2020/2/28 21:36
 */
public class FmdbTypeDescTest {
    public static void main(String[] args) {
//        FmdbTypeDesc fmdbTypeDesc = FmdbTypeDesc.fromString("struct<first:mac,second:int,third:map<string,int>>");
        long l = System.currentTimeMillis();
        FmdbTypeDesc fmdbTypeDesc = FmdbTypeDesc.fromString("int");
        long l2 = System.currentTimeMillis();
        System.out.println("cost:" + (l2 - l));
        FmdbTypeDesc fmdbTypeDesc1 = FmdbTypeDesc.fromString("struct<first:int>");
        long l3 = System.currentTimeMillis();
        System.out.println("cost:" + (l3 - l2));
        System.out.println(fmdbTypeDesc.toString());
    }
}
