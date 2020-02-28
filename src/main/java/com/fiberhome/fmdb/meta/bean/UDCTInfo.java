package com.fiberhome.fmdb.meta.bean;

import java.io.Serializable;

/**
 * @Description 自定义数据类型
 * @Author sjj
 * @Date 19/12/19 下午 04:44
 **/
public class UDCTInfo implements Serializable {

    private static final long serialVersionUID = 4837630759044386409L;
    //自定义数据类型名称
    private String udct_name;
    //写实现类
    private String writer;
    //读实现类
    private String read;
    //对应的基础类型
    private FmdbDataType base_type;

    public UDCTInfo(String udct_name, String writer, String read, FmdbDataType base_type) {
        this.udct_name = udct_name;
        this.writer = writer;
        this.read = read;
        this.base_type = base_type;
    }

    public String getUdct_name() {
        return udct_name;
    }

    public void setUdct_name(String udct_name) {
        this.udct_name = udct_name;
    }

    public String getWriter() {
        return writer;
    }

    public void setWriter(String writer) {
        this.writer = writer;
    }

    public String getRead() {
        return read;
    }

    public void setRead(String read) {
        this.read = read;
    }

    public FmdbDataType getBase_type() {
        return base_type;
    }

    public void setBase_type(FmdbDataType base_type) {
        this.base_type = base_type;
    }

    @Override
    public String toString() {
        return "UDCTInfo{" +
                "udct_name='" + udct_name + '\'' +
                ", writer='" + writer + '\'' +
                ", read='" + read + '\'' +
                ", base_type=" + base_type +
                '}';
    }
}
