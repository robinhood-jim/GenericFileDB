package com.robin.msf.service;

import com.robin.core.base.dao.util.AnnotationRetriever;
import com.robin.core.base.dao.util.PropertyFunction;
import com.robin.core.base.exception.DAOException;
import com.robin.core.base.exception.ServiceException;
import com.robin.core.base.model.BaseObject;
import com.robin.core.base.service.IBaseAnnotationJdbcService;
import com.robin.core.base.util.Const;
import com.robin.core.query.util.PageQuery;
import com.robin.core.sql.util.FilterCondition;
import com.robin.core.sql.util.FilterConditionBuilder;
import com.robin.msf.dao.GenericJdbcDao;
import io.micronaut.inject.qualifiers.Qualifiers;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

import javax.annotation.PostConstruct;
import java.io.Serializable;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import jakarta.transaction.Transactional;
import io.micronaut.transaction.annotation.ReadOnly;

public class AbstractService<V extends BaseObject,P extends Serializable> implements IBaseAnnotationJdbcService<V,P> {
    protected GenericJdbcDao jdbcDao;
    protected Class<V> type;
    protected Class<P> pkType;
    protected Logger logger= LoggerFactory.getLogger(getClass());
    protected AnnotationRetriever.EntityContent entityContent;

    @Inject
    private ApplicationContext applicationContext;
    public AbstractService(){
        Type genericSuperClass = getClass().getGenericSuperclass();
        ParameterizedType parametrizedType;
        if (genericSuperClass instanceof ParameterizedType) {
            parametrizedType = (ParameterizedType) genericSuperClass;
        } else if (genericSuperClass instanceof Class) {
            parametrizedType = (ParameterizedType) ((Class<?>) genericSuperClass).getGenericSuperclass();
        } else {
            throw new IllegalStateException("class " + getClass() + " is not subtype of ParametrizedType.");
        }
        type = (Class) parametrizedType.getActualTypeArguments()[0];
        pkType=(Class) parametrizedType.getActualTypeArguments()[1];
        if(type!=null){
            entityContent= AnnotationRetriever.getMappingTableByCache(type);
        }
    }
    @PostConstruct
    public void init(){
        if(entityContent!=null && entityContent.getJdbcDao()!=null && !entityContent.getJdbcDao().isEmpty()){
            jdbcDao = applicationContext.getBean(GenericJdbcDao.class, Qualifiers.byName(entityContent.getJdbcDao()));
        }else{
            jdbcDao = applicationContext.getBean(GenericJdbcDao.class);
        }
    }

    @Override
    @Transactional
    public P saveEntity(V v) throws ServiceException {
        try{
            return jdbcDao.createVO(v,pkType);
        }catch (DAOException ex){
            throw new ServiceException(ex);
        }
    }

    @Override
    @Transactional
    public int updateEntity(V v) throws ServiceException {
        try{
            return jdbcDao.updateByKey(type,v);
        }catch (DAOException ex){
            throw new ServiceException(ex);
        }
    }

    @Override
    @Transactional
    public int deleteEntity(P[] ps) throws ServiceException {
        try{
            return jdbcDao.deleteVO(type,ps);
        }catch (DAOException ex){
            throw new ServiceException(ex);
        }
    }

    @Override
    @Transactional
    public int deleteByField(String s, Object o) throws ServiceException {
        try{
            return jdbcDao.deleteByField(type,s,o);
        }catch (DAOException ex){
            throw new ServiceException(ex);
        }
    }

    @Override
    @Transactional
    public int deleteByField(PropertyFunction<V, ?> propertyFunction, Object o) throws ServiceException {
        try{
            return jdbcDao.deleteByField(type,propertyFunction,o);
        }catch (DAOException ex){
            throw new ServiceException(ex);
        }
    }

    @Override
    @ReadOnly
    public V getEntity(P p) throws ServiceException {
        try{
            return jdbcDao.getEntity(type,p);
        }catch (DAOException ex){
            throw new ServiceException(ex);
        }
    }

    @Override
    @ReadOnly
    public void queryBySelectId(PageQuery<Map<String, Object>> pageQuery) throws ServiceException {
        try{
            jdbcDao.queryBySelectId(pageQuery);
        }catch (DAOException ex){
            throw new ServiceException(ex);
        }
    }

    @Override
    @ReadOnly
    public List<Map<String, Object>> queryByPageSql(String s, PageQuery<Map<String, Object>> pageQuery) throws ServiceException {
        try{
            return jdbcDao.queryByPageSql(s,pageQuery);
        }catch (DAOException ex){
            throw new ServiceException(ex);
        }
    }

    @Override
    public void executeBySelectId(PageQuery<Map<String, Object>> pageQuery) throws ServiceException {
        try{
            jdbcDao.executeBySelectId(pageQuery);
        }catch (DAOException ex){
            throw new ServiceException(ex);
        }
    }

    @Override
    @ReadOnly
    public void queryBySql(String s, String s1, String[] strings, PageQuery<Map<String, Object>> pageQuery) throws ServiceException {
        try{
            jdbcDao.queryBySql(s,s1,strings,pageQuery);
        }catch (DAOException ex){
            throw new ServiceException(ex);
        }
    }

    @Override
    @ReadOnly
    public List<Map<String, Object>> queryBySql(String s, Object... objects) throws ServiceException {
        try{
            return jdbcDao.queryBySql(s,objects);
        }catch (DAOException ex){
            throw new ServiceException(ex);
        }
    }

    @Override
    @ReadOnly
    public int queryByInt(String s, Object... objects) throws ServiceException {
        try{
            return jdbcDao.queryByInt(s,objects);
        }catch (DAOException ex){
            throw new ServiceException(ex);
        }
    }

    @Override
    @ReadOnly
    public List<V> queryByField(String s, Const.OPERATOR operator, Object... objects) throws ServiceException {
        try{
            return jdbcDao.queryByField(type,s,operator,objects);
        }catch (DAOException ex){
            throw new ServiceException(ex);
        }
    }

    @Override
    @ReadOnly
    public List<V> queryByField(PropertyFunction<V, ?> propertyFunction, Const.OPERATOR operator, Object... objects) throws ServiceException {
        try{
            return jdbcDao.queryByField(type,propertyFunction,operator,objects);
        }catch (DAOException ex){
            throw new ServiceException(ex);
        }
    }

    @Override
    @ReadOnly
    public List<V> queryByFieldOrderBy(String orderByStr, String fieldName, Const.OPERATOR oper, Object... fieldValues) throws ServiceException {
        try{
            return jdbcDao.queryByFieldOrderBy(type,fieldName, oper, orderByStr, fieldValues);
        }catch (DAOException ex){
            throw new ServiceException(ex);
        }
    }

    @Override
    @ReadOnly
    public List<V> queryByFieldOrderBy(String s, PropertyFunction<V, ?> propertyFunction, Const.OPERATOR operator, Object... fieldValues) throws ServiceException {
        List<V> retlist;
        try{
            retlist= jdbcDao.queryByFieldOrderBy(type,propertyFunction, operator,s, fieldValues);
        }
        catch(DAOException ex){
            throw new ServiceException(ex);
        }
        return retlist;
    }

    @Override
    @ReadOnly
    public List<V> queryAll() throws ServiceException {
        List<V> retlist;
        try{
            retlist=jdbcDao.queryAll(type);
        }catch (DAOException e) {
            throw new ServiceException(e);
        }
        return retlist;
    }

    @Override
    @ReadOnly
    public List<V> queryByVO(V v, String s) throws ServiceException {
        try{
            return jdbcDao.queryByVO(type,v,s);
        }catch (DAOException ex){
            throw new ServiceException(ex);
        }
    }

    @Override
    @ReadOnly
    public void queryByCondition(FilterCondition filterCondition, PageQuery<V> pageQuery) {
        try{
            jdbcDao.queryByCondition(type,filterCondition,pageQuery);
        }catch (DAOException ex){
            throw new ServiceException(ex);
        }
    }

    @Override
    public void queryByCondition(FilterConditionBuilder filterConditionBuilder, PageQuery<V> pageQuery) {
        try {
            jdbcDao.queryByCondition(type,filterConditionBuilder.build(),pageQuery);
        }catch (DAOException ex){
            throw new ServiceException(ex);
        }
    }

    @Override
    @ReadOnly
    public List<V> queryByCondition(FilterCondition filterCondition) {
        try{
            PageQuery<V> pageQuery=new PageQuery<>();
            pageQuery.setPageSize(0);
            jdbcDao.queryByCondition(type,filterCondition,pageQuery);
            return pageQuery.getRecordSet();
        }catch (DAOException ex){
            throw new ServiceException(ex);
        }
    }


    @Override
    @ReadOnly
    public V getByField(String s, Const.OPERATOR operator, Object... objects) throws ServiceException {
        try{
            return jdbcDao.getByField(type,s,operator,objects);
        }catch (DAOException ex){
            throw new ServiceException(ex);
        }
    }

    @Override
    @ReadOnly
    public V getByField(PropertyFunction<V, ?> propertyFunction, Const.OPERATOR operator, Object... objects) throws ServiceException {
        try{
            return jdbcDao.getByField(type,propertyFunction,operator,objects);
        }catch (DAOException ex){
            throw new ServiceException(ex);
        }
    }
    @Override
    public int countByCondition(FilterCondition filterCondition) {
        return 0;
    }
}
