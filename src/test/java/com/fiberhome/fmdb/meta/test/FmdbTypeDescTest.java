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
        FmdbTypeDesc fmdbTypeDesc = FmdbTypeDesc.fromString("struct<first:int>");
        System.out.println(fmdbTypeDesc.toString());
    }
}
