package com.fiberhome.fmdb.zookeeper;

/**
 * @Description 回调函数
 * @Author sjj
 * @Date 19/12/18 上午 11:06
 **/
public interface ICallback {
    // 检测是否更新
    enum CheckUpdate {
        OLD(0), NEW(1);
        private int desc;
        CheckUpdate(int desc) {
            this.desc = desc;
        }
        public int getDesc() {
            return desc;
        }
    }
    public void callback(CheckUpdate checkUpdate);
}
