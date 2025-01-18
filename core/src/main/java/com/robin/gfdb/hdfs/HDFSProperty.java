package com.robin.gfdb.hdfs;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.robin.core.base.util.Const;


public class HDFSProperty implements Comparable {
    private String defaultName;
    private Map<String, String> haConfig = new HashMap<String, String>();

    public String getDefaultName() {
        return defaultName;
    }

    public void setDefaultName(String defaultName) {
        this.defaultName = defaultName;
    }

    public Map<String, String> getHaConfig() {
        return haConfig;
    }

    public void setHaConfig(Map<String, String> haConfig) {
        this.haConfig = haConfig;
        getDefaultName(haConfig);
    }

    public void setHaConfigByObj(Map<String, Object> config) {
        Iterator<Map.Entry<String,Object>> iter = config.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<String,Object> entry = iter.next();
            haConfig.put(entry.getKey(), entry.getValue().toString());
        }
        getDefaultName(haConfig);
    }

    private void getDefaultName(Map<String, String> haConfig) {
        if (defaultName == null) {
            if (haConfig.containsKey(Const.HDFS_NAME_HADOOP1)) {
                defaultName = haConfig.get(Const.HDFS_NAME_HADOOP1);
            } else if (haConfig.containsKey(Const.HDFS_NAME_HADOOP2)) {
                defaultName = haConfig.get(Const.HDFS_NAME_HADOOP2);
            }
        }
    }

    @Override
    public int compareTo(Object o) {
        if(equals(o)){
            return 1;
        }
        return 0;
    }

    @Override
    public boolean equals(Object obj) {
        boolean iseq = false;
        if (obj instanceof HDFSProperty) {
            HDFSProperty comp = (HDFSProperty) obj;
            if (this.getDefaultName().equals(comp.getDefaultName())) {
                Iterator<String> iter = this.getHaConfig().keySet().iterator();
                while (iter.hasNext()) {
                    String key = iter.next();
                    if (comp.getHaConfig().containsKey(key) && !this.getHaConfig().get(key).equals(comp.getHaConfig().get(key))) {
                        return false;
                    }
                }
                iseq = true;
            }
        }
        return iseq;
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }
}
