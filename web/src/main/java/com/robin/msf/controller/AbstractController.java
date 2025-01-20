package com.robin.msf.controller;

import cn.hutool.core.lang.Assert;
import com.robin.core.base.exception.ServiceException;
import com.robin.core.base.exception.WebException;
import com.robin.core.base.model.BaseObject;
import com.robin.core.base.util.MessageUtils;
import com.robin.core.convert.util.ConvertUtil;
import com.robin.core.query.util.PageQuery;
import com.robin.msf.bean.ApplicationContextHolder;
import com.robin.msf.service.AbstractService;
import io.micronaut.http.HttpRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.ObjectUtils;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

public abstract class AbstractController <O extends BaseObject, P extends Serializable, S extends AbstractService<O,P>>  {
    protected Class<O> voType;
    protected Class<P> pkType;
    protected Class<S> serviceType;
    protected String pkColumn = "id";
    protected String defaultOrderByField="create_tm";
    protected String deleteColumn="delete_tag";
    protected Field deleteField=null;
    protected S service;
    protected Logger log = LoggerFactory.getLogger(getClass());
    protected static final String COL_MESSAGE="message";
    protected static final String COL_SUCCESS="success";
    protected static final String COL_COED="code";
    protected static final String COL_DATA="data";
    protected Method valueOfMethod;

    protected AbstractController(){
        Type genericSuperClass = getClass().getGenericSuperclass();
        ParameterizedType parametrizedType;
        if ((genericSuperClass instanceof ParameterizedType)) {
            parametrizedType = (ParameterizedType) genericSuperClass;
        } else {
            if ((genericSuperClass instanceof Class)) {
                parametrizedType = (ParameterizedType) ((Class) genericSuperClass).getGenericSuperclass();
            } else {
                throw new IllegalStateException("class " + getClass() + " is not subtype of ParametrizedType.");
            }
        }
        this.serviceType = ((Class) parametrizedType.getActualTypeArguments()[0]);
        this.voType = ((Class) parametrizedType.getActualTypeArguments()[2]);
        this.pkType = ((Class) parametrizedType.getActualTypeArguments()[3]);
        try {
            if(!pkType.isAssignableFrom(String.class)) {
                valueOfMethod = this.pkType.getMethod("valueOf", String.class);
            }
            if (this.serviceType != null) {
                this.service= ApplicationContextHolder.getBean(serviceType);
            }
        } catch (Exception ex) {
            log.error("{0}", ex);
        }

    }
    protected Map<String, Object> doSave(O obj) {
        Map<String, Object> retMap = new HashMap<>();
        try {
            P pk=this.service.saveEntity(obj);
            constructRetMap(retMap);
            doAfterAdd(obj,pk, retMap);
        } catch (ServiceException ex) {
            this.log.error("{0}", ex);
            wrapResponse(retMap, ex);
        }
        return retMap;
    }
    protected Map<String, Object> doSave(Map<String,Object> paramMap) {
        Map<String, Object> retMap = new HashMap<>();
        try {
            O object=this.voType.newInstance();
            ConvertUtil.convertToModel(object,paramMap);
            P pk=this.service.saveEntity(object);
            constructRetMap(retMap);
            doAfterAdd(object,pk, retMap);
        } catch (Exception ex) {
            this.log.error("{0}", ex);
            wrapResponse(retMap, ex);
        }
        return retMap;
    }

    protected Map<String, Object> doView(P id) {
        Map<String, Object> retMap = new HashMap<String, Object>();
        try {
            O object = service.getEntity(id);
            retMap = new HashMap<>();
            doAfterView(object, retMap);
            constructRetMap(retMap);
        } catch (Exception e) {
            log.error("{0}", e);
            wrapFailed(retMap, e);
        }
        return retMap;
    }

    protected Map<String, Object> doEdit(P id) {
        Map<String, Object> retMap = new HashMap<String, Object>();
        try {
            BaseObject object = service.getEntity(id);
            doAfterEdit(object, retMap);
            constructRetMap(retMap);
        } catch (Exception e) {
            log.error("{0}", e);
            wrapFailed(retMap, e);
        }
        return retMap;
    }

    protected Map<String, Object> doUpdate(Map<String,Object> paramMap,P id) {
        Map<String, Object> retMap = new HashMap<>();
        try {
            O originObj= this.voType.newInstance();
            ConvertUtil.convertToModel(originObj,paramMap);
            updateWithOrigin(id, retMap, originObj);
        } catch (Exception ex) {
            log.error("{0}", ex);
            wrapFailed(retMap, ex);
        }
        return retMap;
    }

    private void updateWithOrigin(P id, Map<String, Object> retMap, O originObj) throws Exception {
        O updateObj = service.getEntity(id);
        ConvertUtil.convertToModelForUpdate(updateObj, originObj);
        service.updateEntity(updateObj);
        doAfterUpdate(updateObj, retMap);
        constructRetMap(retMap);
    }

    protected Map<String, Object> doUpdate(O base,P id) {
        Map<String, Object> retMap = new HashMap<>();
        try {
            updateWithOrigin(id, retMap, base);
        } catch (Exception ex) {
            log.error("{0}", ex);
            wrapFailed(retMap, ex);
        }
        return retMap;
    }

    protected void doAfterAdd(BaseObject obj,P pk, Map<String, Object> retMap) {
        retMap.put("data",obj);
    }

    protected void doAfterView(BaseObject obj, Map<String, Object> retMap) {
        retMap.put("data", obj);
    }

    protected void doAfterEdit(BaseObject obj, Map<String, Object> retMap) {
        retMap.put("data", obj);
    }

    protected void doAfterUpdate(BaseObject obj, Map<String, Object> retMap) {
    }

    protected void doAfterQuery(PageQuery query, Map<String, Object> retMap) {
        retMap.put("recordCount", query.getRecordCount());
        retMap.put("pageNumber", query.getPageNumber());
        retMap.put("pageCount", query.getPageCount());
        retMap.put("pageSize", query.getPageSize());
        retMap.put("data",query.getRecordSet());
    }

    protected void doAfterDelete(P[] ids, Map<String, Object> retMap) {

    }


    protected Map<String, Object> doDelete(P[] ids) {
        Map<String, Object> retMap = new HashMap();
        try {
            this.service.deleteEntity(ids);
            doAfterDelete(ids, retMap);
            constructRetMap(retMap);
        } catch (Exception ex) {
            wrapFailed(retMap, ex);
        }
        return retMap;
    }



    protected  P[] parseId(String ids) throws ServiceException {
        P[] array=null;
        try {
            Assert.notNull(ids,"input id is null");
            Assert.isTrue(ids.length()>0,"input ids is empty");
            String[] idsArr = ids.split(",");
            array=(P[])java.lang.reflect.Array.newInstance(pkType,idsArr.length);
            for (int i = 0; i < idsArr.length; i++) {
                if (valueOfMethod != null) {
                    P p = pkType.newInstance();
                    valueOfMethod.invoke(p, idsArr[i]);
                    array[i]=p;
                }else{
                    array[i]=(P)idsArr[i];
                }

            }
        } catch (Exception ex) {
            throw new ServiceException(ex);
        }
        return array;
    }
    protected abstract String wrapQuery(HttpRequest<?> request, PageQuery query);
    protected void constructRetMap(Map<String, Object> retMap)
    {
        retMap.put(COL_SUCCESS, true);
        retMap.put(COL_COED, 0);
    }

    protected void wrapFailed( Map<String, Object> retMap, Exception ex)
    {
        if(ex instanceof ServiceException){
            retMap.put(COL_COED,((ServiceException)ex).getRetCode());
            retMap.put(COL_MESSAGE, MessageUtils.getMessage(((ServiceException)ex).getRetCode()));
        }
        else if(ex instanceof WebException){
            retMap.put(COL_COED,((WebException)ex).getRetCode());
            retMap.put(COL_MESSAGE,MessageUtils.getMessage(((WebException)ex).getRetCode()));
        }else {
            retMap.put(COL_SUCCESS, false);
            retMap.put(COL_MESSAGE, ex.getMessage());
        }
    }
    protected void wrapFailed( Map<String, Object> retMap, String message)
    {
        retMap.put(COL_SUCCESS, false);
        retMap.put(COL_MESSAGE, message);
    }



    protected Map<String, Object> wrapSuccess(String displayMsg)
    {
        Map<String, Object> retmap = new HashMap<>();
        retmap.put(COL_SUCCESS, true);
        retmap.put(COL_MESSAGE, displayMsg);
        return retmap;
    }

    protected void wrapSuccessMap(Map<String, Object> retmap, String displayMsg)
    {
        retmap.put(COL_SUCCESS, true);
        retmap.put(COL_MESSAGE, displayMsg);
    }
    protected void  wrapError(Map<String, Object> retmap,String message)
    {
        retmap.put(COL_SUCCESS, false);
        retmap.put(COL_MESSAGE, message);
    }
    protected Map<String, Object> wrapObject(Object object)
    {
        Map<String, Object> retmap = new HashMap<>();
        retmap.put(COL_SUCCESS, true);
        retmap.put(COL_DATA, object);
        return retmap;
    }

    protected Map<String, Object> wrapError(Exception ex)
    {
        Map<String, Object> retmap = new HashMap<>();
        retmap.put(COL_SUCCESS, false);
        retmap.put(COL_MESSAGE, ex.getMessage());
        return retmap;
    }


    protected PageQuery wrapPageQuery(Map<String,Object> paramMap){
        PageQuery query = new PageQuery();
        try
        {
            ConvertUtil.mapToObject(query, paramMap);
        }
        catch (Exception ex)
        {
            ex.printStackTrace();
        }
        return query;
    }



    public static Map<String,Object> wrapSuccessMsg(String message){
        Map<String,Object> retmap=new HashMap<>();
        retmap.put(COL_SUCCESS, true);
        if(message!=null && !message.trim().isEmpty()){
            retmap.put(COL_MESSAGE,message);
        }
        return retmap;
    }
    public static Map<String,Object> wrapFailedMsg(String message){
        Map<String,Object> retmap=new HashMap<>();
        retmap.put(COL_SUCCESS, false);
        if(message!=null && !message.trim().isEmpty()){
            retmap.put(COL_MESSAGE,message);
        }
        return retmap;
    }
    public static Map<String,Object> wrapFailedMsg(Exception ex){
        Map<String,Object> retmap=new HashMap<>();
        retmap.put(COL_SUCCESS, false);
        if(ex!=null){
            retmap.put(COL_MESSAGE,ex.getMessage());
        }
        return retmap;
    }
    protected  Long[] parseLongId(String ids) throws ServiceException {
        Assert.isTrue(!ObjectUtils.isEmpty(ids),"input ids is empty");
        Long[] array;
        try {
            String[] idsArr = ids.split(",");
            array=new Long[idsArr.length];
            for (int i = 0; i < idsArr.length; i++) {
                array[i]=Long.valueOf(idsArr[i]);
            }
        } catch (Exception ex) {
            throw new ServiceException(ex);
        }
        return array;
    }
    protected void wrapResponse(Map<String,Object> retmap,Exception ex){
        if(ex!=null){
            wrapFailed(retmap,ex);
        }else
        {
            wrapSuccessMap(retmap,COL_SUCCESS);
        }
    }
}
