package com.fiberhome.fmdb.meta.bean;

/**
 * @Description 压缩方式
 * @Author sjj
 * @Date 19/11/12 上午 10:45
 **/
public enum CompressionType {
    NONE,
    ZLIB,
    SNAPPY,
    LZO,
    LZ4,
    ZSTD;

    private CompressionType() {
    }
}
