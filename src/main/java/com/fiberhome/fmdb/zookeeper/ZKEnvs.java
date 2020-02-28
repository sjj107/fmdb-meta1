package com.fiberhome.fmdb.zookeeper;

import java.util.concurrent.CountDownLatch;

/**
 * @Description TODO
 * @Author sjj
 * @Date 19/12/18 下午 01:43
 **/
public class ZKEnvs {

    /**
     * DB状态监控计数器s
     */
    private static CountDownLatch statusLatch = new CountDownLatch(1);
    public static CountDownLatch getStatusLatch()
    {
        return statusLatch;
    }

    public static void setStatusLatch(CountDownLatch dbStatusLatch)
    {
        ZKEnvs.statusLatch = dbStatusLatch;
    }
}
