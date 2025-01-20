package com.robin.gfdb.meta.controller;

import cn.hutool.core.lang.Assert;
import com.robin.core.base.exception.WebException;
import com.robin.core.query.util.PageQuery;
import com.robin.gfdb.meta.model.DataCollectionDefine;
import com.robin.gfdb.meta.service.DataCollectionDefineService;
import com.robin.gfdb.meta.service.FileStorageDefineService;
import com.robin.msf.controller.AbstractController;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.annotation.*;
import jakarta.inject.Inject;

import java.util.Map;

@Controller("/datacollection")
public class DataCollectionDefineController extends AbstractController<DataCollectionDefine,Long, DataCollectionDefineService> {
    @Inject
    private FileStorageDefineService storageDefineService;

    @Post("/save")
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
        return doView(id);
    }


    @Override
    protected String wrapQuery(HttpRequest<?> request, PageQuery query) {
        return null;
    }
}
