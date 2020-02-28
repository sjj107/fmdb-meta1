package com.fiberhome.fmdb.quartz.job;

import com.fiberhome.fmdb.common.CommonUtil;
import com.fiberhome.fmdb.common.Constant;
import com.fiberhome.fmdb.manifest.main.UpdateManifestMain;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * @Description 更新manifest数据的job
 * @Author sjj
 * @Date 19/10/31 上午 10:50
 **/
public class UpdateManifestJob implements Job {
    /**
     * 日志
     */
    private static Logger logger = LoggerFactory.getLogger("fmdbquartz");

    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        logger.debug("开始重命名tmp文件");
        File[] dbDirs = UpdateManifestMain.dataDir.listFiles();
        String dbName;
        String partName;
        String orcTmpName;
        for (File dbDir : dbDirs) {
            if (!dbDir.isDirectory()) {
                logger.error("[{}]不是目录！", dbDir.getAbsolutePath());
                continue;
            }
            dbName = dbDir.getName();
            File[] tableDirs = dbDir.listFiles();
            for (File tableDir : tableDirs) {
                if (!tableDir.isDirectory()) {
                    logger.error("[{}]不是目录！", tableDir.getAbsolutePath());
                    continue;
                }
                File[] partDirs = tableDir.listFiles();
                for (File partDir : partDirs) {
                    if (!partDir.isDirectory()) {
                        logger.error("[{}]不是目录！", partDir.getAbsolutePath());
                        continue;
                    }
                    partName = partDir.getName();
                    File[] orcFiles = partDir.listFiles(Constant.ORC_TMP_FILE);
                    for (File orcTmpFile : orcFiles) {
                        orcTmpName = orcTmpFile.getName();
                        boolean b = UpdateManifestMain.metaClient.storageOrcInfo(UpdateManifestMain.dataDir.getAbsolutePath(), dbName, tableDir.getName(), partName, orcTmpName, true);
                        //将orc临时文件重命名为最终文件
                        if (b) {
                            boolean b1 = CommonUtil.INSTANCE.reNameOrcTmpFile(orcTmpFile);
                            logger.debug("重命名[{}]文件" + (b1 ? "成功" : "失败"), orcTmpFile.getAbsolutePath());
                            if (!b1) {
                                UpdateManifestMain.metaClient.deleteTableManifest(dbName, tableDir.getName());
                            }
                        }
                    }
                }

            }
        }

    }
}
