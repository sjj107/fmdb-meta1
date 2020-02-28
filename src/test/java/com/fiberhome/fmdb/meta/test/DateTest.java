package com.fiberhome.fmdb.meta.test;

import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * @Description 时间测试类
 * @Author sjj
 * @Date 19/11/12 下午 04:51
 **/
public class DateTest {
    public static void main(String[] args) {
        Calendar calendar = Calendar.getInstance();
        calendar.set(Calendar.MONTH, 0);
        calendar.set(Calendar.DAY_OF_MONTH, 1);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);

//        calendar.add(Calendar.MONTH, -1 + 1);
        SimpleDateFormat sdf = null;
        sdf = new SimpleDateFormat("yyyyMMdd");
        System.out.println(calendar.getTime().getTime() / 1000);
    }
}
