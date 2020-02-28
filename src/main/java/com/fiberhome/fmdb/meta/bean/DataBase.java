package com.fiberhome.fmdb.meta.bean;

import java.io.Serializable;
import java.util.Map;

/**
 * @Description 数据库
 * @Author sjj
 * @Date 19/11/07 下午 08:58
 **/
public class DataBase implements Serializable {
    private static final long serialVersionUID = 3803088778649444333L;
    /**
     * 库名
     */
    private String name;
    /**
     * 描述
     */
    private String description;
    /**
     * 库的数据路径
     */
    private String locationUri; // required
    /**
     * 库的参数信息
     */
    private Map<String,String> parameters; // required
}
