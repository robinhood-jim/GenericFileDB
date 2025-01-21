package com.robin.gfdb.meta.controller;

import cn.hutool.core.lang.Assert;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.robin.core.base.exception.WebException;
import com.robin.core.convert.util.ConvertUtil;
import com.robin.core.query.util.PageQuery;
import com.robin.gfdb.meta.model.DataCollectionDefine;
import com.robin.gfdb.meta.model.FileStorageDefine;
import com.robin.gfdb.meta.service.DataCollectionDefineService;
import com.robin.gfdb.meta.service.FileStorageDefineService;
import com.robin.msf.controller.AbstractController;
import com.robin.msf.json.GsonUtil;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.annotation.*;
import jakarta.inject.Inject;

import java.util.HashMap;
import java.util.Map;

@Controller("/datacollection")
public class DataCollectionDefineController extends AbstractController<DataCollectionDefine,Long, DataCollectionDefineService> {
    @Inject
    private FileStorageDefineService storageDefineService;
    private Gson gson= GsonUtil.getGson();

    @Post("/save")
    @Consumes
    @Produces
    public Map<String,Object> doSave(HttpRequest<?> request, @Body Map<String,Object> reqMap){
        try{
            Assert.notNull(reqMap.get("fields"));
            Long id= service.doSaveCollection(reqMap);
            return wrapObject(id);
        }catch (WebException ex){
            return wrapError(ex);
        }
    }
    @Get("/view/{id}")
    public Map<String,Object> doView(HttpRequest<?> request, @PathVariable Long id){
        try {
            DataCollectionDefine define = service.getEntity(id);
            Assert.notNull(define, "id not found");
            Assert.notNull(define.getStorageId(), "storage id is empty");
            FileStorageDefine storageDefine = storageDefineService.getEntity(define.getStorageId());
            Assert.notNull(storageDefine, "storage id not found");
            Map<String, Object> retMap = new HashMap<>();
            ConvertUtil.mapToObject(define, retMap);
            retMap.put("storageType",storageDefine.getStorageType());
            retMap.put("storageParam",gson.fromJson(storageDefine.getConfigParam(),new TypeToken<Map<String,Object>>(){}.getType()));
            return retMap;
        }catch (Exception ex){
            throw new WebException(ex);
        }
    }



    @Override
    protected String wrapQuery(HttpRequest<?> request, PageQuery query) {
        return null;
    }
}
