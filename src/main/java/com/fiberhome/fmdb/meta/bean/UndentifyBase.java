package com.fiberhome.fmdb.meta.bean;

import org.apache.orc.UndefineEncodingBean;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class UndentifyBase {

    private   static  volatile UndentifyBase instance;

    private static HashMap<String,String> hashMap=new HashMap<>();

    private UndentifyBase(){}


    public static UndentifyBase getInstance(List<UndefineEncodingBean> list){

        if (instance==null){
                synchronized (org.apache.orc.util.UndentifyBase.class){
                    if (instance==null){
                        instance=new UndentifyBase();
                        Map<String, String> hashMap = instance.getHashMap();
                        for (int i = 0; i < list.size(); i++) {
                            hashMap.put(list.get(i).getUndentifyCloumn().toUpperCase(),list.get(i).getType());
                        }
                    }
                }
        }
        return instance;
    }

    public static UndentifyBase getInstance(){
        if (instance==null){
            return new UndentifyBase();
        }else {
            return instance;
        }
    }

    public Map<String, String> getHashMap() {
        return hashMap;
    }

}
