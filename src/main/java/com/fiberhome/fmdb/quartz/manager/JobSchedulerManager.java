package com.fiberhome.fmdb.quartz.manager;

import com.fiberhome.fmdb.quartz.job.LoadManifestJob;
import com.fiberhome.fmdb.quartz.job.UpdateManifestJob;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Description 调度管理
 * @Author sjj
 * @Date 19/10/31 上午 10:38
 **/
public class JobSchedulerManager {
    /**
     * 日志
     */
    private static Logger logger = LoggerFactory.getLogger("fmdbquartz");

    /**
     * 任务分配工厂类
     */
    private SchedulerFactory schedulerFactory;

    /**
     * 调度
     */
    Scheduler scheduler = null;

    public JobSchedulerManager() {
        this.schedulerFactory = new StdSchedulerFactory();
    }

    /**
     * 存储orc的元数据信息
     *
     * @param cron   周期
     * @param jobMap jobMap
     */
    public void startUpdateManifest(String cron, JobDataMap jobMap) {
//        try {
//            new UpdateManifestJob().execute(null);
//        } catch (JobExecutionException e) {
//            e.printStackTrace();
//        }
        startJob("updatemanifest", cron, UpdateManifestJob.class, jobMap);
    }

    public void startLoadManifest(String cron, JobDataMap jobMap) {
        startJob("loadmanifest", cron, LoadManifestJob.class, jobMap);
    }

    private void startJob(String identity, String cron, Class clazz, JobDataMap jobMap) {
        JobDetail job = JobBuilder.newJob(clazz).withIdentity(identity, "fmdb-meta").build();
        if (jobMap != null) {
            job.getJobDataMap().putAll(jobMap);
        }
        Trigger trigger;
        try {
            scheduler = schedulerFactory.getScheduler();
            trigger = TriggerBuilder.newTrigger()
                    .withIdentity(identity + "Trigger", "fbdb-meta")
                    .withSchedule(CronScheduleBuilder.cronSchedule(cron))
                    .startNow()
                    .build();

            scheduler.scheduleJob(job, trigger);
            scheduler.start();
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }
}
