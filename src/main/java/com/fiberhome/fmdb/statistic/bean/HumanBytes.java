package com.fiberhome.fmdb.statistic.bean;

/**
 * 将字节大小转换成便于人类读的信息，如：
 * 1024bytes -> 1K
 * 12304504bytes  -> 11.73M
 */
public class HumanBytes {
    private long origSize;
    private float size;
    private Unit unit;
    private final int SIZE = 1024;

    public HumanBytes(long origSize) {
        this.origSize = origSize;
        genSuitableSize(origSize);
    }

    public long getOrigSize() {
        return origSize;
    }

    public void setOrigSize(long origSize) {
        this.origSize = origSize;
        genSuitableSize(origSize);
    }

    private void genSuitableSize(long size) {
        if (size < SIZE) {
            this.size = size;
            this.unit = Unit.B;
        } else if (size < SIZE * SIZE) {
            this.size = (float) size / SIZE;
            this.unit = Unit.K;
        } else if (size < SIZE * SIZE * SIZE) {
            this.size = (float) size / SIZE / SIZE;
            this.unit = Unit.M;
        } else {
            this.size = (float) size / SIZE / SIZE / SIZE;
            this.unit = Unit.G;
        }
    }

	@Override
	public String toString() {
		return ""+size+unit;
	}
    
    
}
