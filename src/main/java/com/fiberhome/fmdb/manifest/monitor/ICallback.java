package com.fiberhome.fmdb.manifest.monitor;

/**
 * 回调函数
 */
@Deprecated
public interface ICallback {
    //检查orc元数据是否变化
    enum CheckUpdate {
        OLD(0), NEW(1);
        private int index;

        CheckUpdate(int index) {
            this.index = index;
        }

        public int getIndex() {
            return index;
        }
    }

    public void callback(CheckUpdate checkUpdate);
}
