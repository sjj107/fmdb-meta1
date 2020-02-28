package com.fiberhome.fmdb.zookeeper;

import com.fiberhome.fmdb.common.Constant;
import com.fiberhome.fmdb.meta.conf.FMDBMetaConf;
import org.apache.commons.lang.StringUtils;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.*;

/**
 * @Description zk连接
 * @Author sjj
 * @Date 19/12/17 下午 03:46
 **/
public class ZKManager implements Watcher {
    private static Logger logger = LoggerFactory.getLogger("fmdbmeta");

    private ZKManager() {

    }

    private static volatile ZKManager instance;

    public static ZKManager getInstance() {
        if (instance == null) {
            synchronized (ZKManager.class) {
                if (instance == null) {
                    instance = new ZKManager();
                        instance.connectZookeeper();
                }
            }
        }
        return instance;
    }

    private ZooKeeper zooKeeper;

    private static final int SESSION_TIME_OUT = 30000;
    private CountDownLatch countDownLatch = new CountDownLatch(1);

    @Override
    public void process(WatchedEvent event) {
        if (event.getState() == Event.KeeperState.SyncConnected) {
            countDownLatch.countDown();
        }else if(event.getState() == Event.KeeperState.Expired){
            logger.info("ZK  session expired ,now reconnect ...");
            this.closeConnect();
            this.connectZookeeper();

        }
    }

    public void registerZKMonitor(String path, ICallback callback, Watcher watcher) {
        ExecutorService pool =
                new ThreadPoolExecutor(2, 4, 1000, TimeUnit.MICROSECONDS, new ArrayBlockingQueue<Runnable>(5),
                        r -> new Thread(r, "ZKDBMonitorPool-" + r.hashCode()), new ThreadPoolExecutor.CallerRunsPolicy());
        pool.execute(new ZKDBMonitor(path, callback, this, watcher));
    }

    /**
     * 连接zk
     *
     *
     * @throws IOException
     * @throws InterruptedException
     */
    private void connectZookeeper(){
        try {
        zooKeeper = new ZooKeeper(FMDBMetaConf.getInstance().zk_url, SESSION_TIME_OUT, this);
        countDownLatch.await();
        } catch (IOException e) {
            logger.error("连接zk错误", e);
        } catch (InterruptedException e) {
            logger.error("连接zk错误", e);
        }
    }


    /**
     * 判断节点是否存在
     *
     * @param path
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
    public boolean exists(String path) throws KeeperException, InterruptedException {
//        this.zooKeeper.e
        Stat exists = this.zooKeeper.exists(path, false);
        return exists != null;
    }

    /**
     * 创建永久节点
     *
     * @param path
     * @param data
     * @return
     */
    public void createPNode(String path, byte[] data) throws KeeperException, InterruptedException {
        this.zooKeeper.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    public void createPersistentMultiPath(String path, byte[] data) {
        String[] pArray = path.split(Constant.SLASH);
        StringBuilder pathBuilder = new StringBuilder();
        int index = 0;
        for (String p : pArray) {

            if (!StringUtils.EMPTY.equals(p)) {
                pathBuilder.append(Constant.SLASH).append(p);
                try {
                    boolean exists = exists(pathBuilder.toString());

                    if (!exists) {// 表示不存在
                        createPNode(pathBuilder.toString(), new byte[]{});
                        if (index == pArray.length - 1) {
                            setData(pathBuilder.toString(), data);
                        }
                    }
                } catch (Exception e) {
                    try {
                        boolean exists = exists(pathBuilder.toString());

                        if (!exists) {
                            throw e;
                        }
                    } catch (KeeperException | InterruptedException e1) {

                    }

                }
            }
            index++;
        }
    }

    /**
     * 获取路径下的所有子节点
     *
     * @param path
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
    public List<String> getChildren(String path, Watcher watcher) throws KeeperException, InterruptedException {
        List<String> children = this.zooKeeper.getChildren(path, watcher);
        return children;
    }

    /**
     * 获取节点上的数据
     *
     * @param path
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
    public String getData(String path, Watcher watcher) throws KeeperException, InterruptedException {
        byte[] data = this.zooKeeper.getData(path, watcher, null);
        if (data == null) {
            return null;
        }
        return new String(data);
    }

    /**
     * 设置节点信息
     *
     * @param path 路径
     * @param data 数据
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
    public Stat setData(String path, byte[] data) throws KeeperException, InterruptedException {
        Stat stat = this.zooKeeper.setData(path, data, -1);
        return stat;
    }

    /**
     * 删除节点
     *
     * @param path
     * @throws KeeperException
     * @throws InterruptedException
     */
    public void deleteNode(String path) throws KeeperException, InterruptedException {
        this.zooKeeper.delete(path, -1);
    }

    /**
     * 删除该节点下所有的节点，包括本节点
     *
     * @param path
     * @throws InterruptedException
     * @throws KeeperException
     */
    public void deleteMultiNode(String path)
            throws InterruptedException, KeeperException {
        cleanNodeChildren(path);
        try {
            deleteNode(path);
        } catch (KeeperException e) {
            if (e.code() == KeeperException.Code.NONODE) {
                // 正常
            } else {
                throw e;
            }
        }
    }

    /**
     * 清空该节点下所有的节点
     *
     * @param path
     * @throws KeeperException
     * @throws InterruptedException
     */
    public void cleanNodeChildren(String path) throws KeeperException, InterruptedException {
        while (!getChildren(path, null).isEmpty()) {
            cleanLeafNode(path);
        }
    }

    /**
     * 删除该节点下所有的叶子节点
     *
     * @param path
     * @throws KeeperException
     * @throws InterruptedException
     */
    public void cleanLeafNode(String path) throws KeeperException, InterruptedException {
        boolean existPath = exists(path);
        if (existPath) {
            try {
                deleteNode(path);
            } catch (KeeperException.NotEmptyException e) {
                List<String> children = getChildren(path, null);
                for (String child : children) {
                    cleanLeafNode(path + Constant.SLASH + child);
                }
            }
        }
    }

    /**
     * 关闭连接
     *
     * @throws InterruptedException
     */
    public void closeConnect(){

        if (zooKeeper != null) {
            try {
                zooKeeper.close();
            } catch (InterruptedException e) {
                logger.error("关闭zk连接错误", e);
            }
        }
    }
}
