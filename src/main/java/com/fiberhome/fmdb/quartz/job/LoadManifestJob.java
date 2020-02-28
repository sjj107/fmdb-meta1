package com.fiberhome.fmdb.quartz.job;

import com.fiberhome.fmdb.manifest.OrcMetaClient.IOrcMetaCache;
import com.fiberhome.fmdb.meta.tool.IFMDBMetaClient;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

/**
 * @Description 加载manifest数据的job
 * @Author sjj
 * @Date 19/11/04 下午 06:33
 **/
public class LoadManifestJob implements Job {
    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        IOrcMetaCache cache = (IOrcMetaCache)jobExecutionContext.getJobDetail().getJobDataMap().get("cache");
        cache.reloadAllOrcCache();
    }
}
