package com.robin.msf.service;

import com.google.gson.Gson;
import com.robin.core.base.exception.ServiceException;
import com.robin.core.convert.util.ConvertUtil;
import com.robin.gfdb.core.service.AbstractService;


import com.robin.gfdb.meta.model.DataCollectionDefine;
import com.robin.gfdb.utils.json.GsonUtil;
import jakarta.inject.Singleton;

import javax.transaction.Transactional;
import java.util.List;
import java.util.Map;

@Singleton
public class DataCollectionDefineService extends AbstractService<DataCollectionDefine,Long> {
    private static Gson gson= GsonUtil.getGson();

    @Transactional
    public Long doSaveCollection(Map<String,Object> reqMap){
        List<Map<String,Object>> fields= (List<Map<String, Object>>) reqMap.get("fields");
        try{
            DataCollectionDefine define=new DataCollectionDefine();
            ConvertUtil.convertToModel(define,reqMap);
            define.setFields(gson.toJson(fields));
            return saveEntity(define);
        }catch (Exception ex){
            throw new ServiceException(ex);
        }
    }
}
