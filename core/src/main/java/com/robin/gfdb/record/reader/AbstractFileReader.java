package com.robin.gfdb.record.reader;

import com.robin.core.base.exception.MissingConfigException;
import com.robin.core.base.util.Const;
import com.robin.core.base.util.ResourceConst;
import com.robin.core.fileaccess.meta.DataCollectionMeta;
import com.robin.core.fileaccess.meta.DataSetColumnMeta;
import com.robin.gfdb.sql.calculate.Calculator;
import com.robin.gfdb.sql.filter.CommRecordFilter;
import com.robin.gfdb.sql.parser.CommSqlParser;
import com.robin.gfdb.sql.parser.SqlSegment;
import com.robin.gfdb.storage.AbstractFileSystem;
import com.robin.gfdb.storage.ApacheVfsFileSystem;
import com.robin.gfdb.utils.arithmetic.PolandNotationUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.*;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public abstract class AbstractFileReader implements IDataFileReader{
    protected BufferedReader reader;
    protected String identifier;
    protected DataCollectionMeta colmeta;
    protected AbstractFileSystem fileSystem;
    protected InputStream inputStream;
    protected Map<String, Object> cachedValue = new HashMap<>();
    protected Map<String, Object> newRecord = new ConcurrentHashMap<>();
    protected Map<String, DataSetColumnMeta> columnMap = new HashMap<>();
    protected DateTimeFormatter formatter=DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    //if using BufferedReader as input.only csv json format must set this to true
    protected boolean useBufferedReader=false;
    protected boolean useRawInputStream=false;
    protected boolean useOrderBy=false;
    protected boolean useGroupBy=false;
    private  boolean useFilter=false;
    private String filterSql;
    private SqlSegment segment;
    protected String defaultNewColumnPrefix = "N_COLUMN";
    protected Iterator<Map.Entry<String,Map<String,Object>>> groupIter;
    protected Map<String,Map<String,Object>> groupByMap=new HashMap<>();

    public AbstractFileReader(DataCollectionMeta colmeta,AbstractFileSystem fileSystem){
        this.colmeta=colmeta;
        this.fileSystem=fileSystem;
        for (DataSetColumnMeta meta : colmeta.getColumnList()) {
            columnMap.put(meta.getColumnName(), meta);
            if (Const.META_TYPE_FORMULA.equals(meta.getColumnType())) {
                meta.setColumnType(Const.META_TYPE_DOUBLE);
            }
        }
        if (!CollectionUtils.isEmpty(colmeta.getResourceCfgMap()) && !ObjectUtils.isEmpty(colmeta.getResourceCfgMap().get(ResourceConst.STORAGEFILTERSQL))) {
            withFilterSql(colmeta.getResourceCfgMap().get(ResourceConst.STORAGEFILTERSQL).toString());
        }
    }

    @Override
    public void init() throws IOException {
        Assert.notNull(fileSystem,"FileSystem is missing");
        if(useBufferedReader){
            Pair<BufferedReader, InputStream> pair = fileSystem.getInResourceByReader(colmeta.getPath());
            this.reader = pair.getKey();
            this.inputStream = pair.getValue();
        }else{
            if(!useRawInputStream) {
                this.inputStream = fileSystem.getInResourceByStream(colmeta.getPath());
            }else{
                this.inputStream=fileSystem.getRawInputStream(colmeta.getPath());
            }
        }
    }
    @Override
    public void close() throws IOException {
        if (reader != null) {
            reader.close();
        }
        if (inputStream != null) {
            inputStream.close();
        }
        PolandNotationUtil.freeMem();
        fileSystem.close();
    }
    @Override
    public boolean hasNext() {
        try {
            // no order by
            if(!useOrderBy && !useGroupBy) {

                pullNext();
                while (!CollectionUtils.isEmpty(cachedValue) && useFilter && !CommRecordFilter.doesRecordAcceptable(segment, cachedValue)) {
                    pullNext();
                }
                if(CollectionUtils.isEmpty(cachedValue)){
                    return false;
                }
                if (segment != null && (!segment.isIncludeAllOriginColumn() && !CollectionUtils.isEmpty(segment.getSelectColumns()))) {
                    newRecord.clear();
                    CommRecordFilter.doAsyncCalculator(segment, cachedValue, newRecord);
                }
                return !CollectionUtils.isEmpty(cachedValue);
            }else{
                //capture all record to offHeap
                if(CollectionUtils.isEmpty(groupByMap)) {
                    groupOrderByInit();
                }
                newRecord.clear();
                if(groupIter.hasNext()) {
                    newRecord.putAll(groupIter.next().getValue());
                    if(!CollectionUtils.isEmpty(segment.getHaving())) {
                        Number baseVal=(Number)((SqlLiteral)((SqlBasicCall)segment.getHavingCause()).getOperandList().get(1)).getValue();
                        while (!CommRecordFilter.cmpNumber(segment.getHavingCause().getKind(),(Number) newRecord.get(getHavingColumnName()),baseVal)){
                            newRecord.clear();
                            newRecord.putAll(groupIter.next().getValue());
                        }
                    }
                }
                return !CollectionUtils.isEmpty(newRecord);
            }
        } catch (Exception ex) {
            throw new MissingConfigException(ex);
        }
    }

    @Override
    public Map<String, Object> next() {
        return !CollectionUtils.isEmpty(newRecord) ? newRecord : cachedValue;
    }

    @Override
    public String getIdentifier() {
        return identifier;
    }
    public void setIdentifier(String identifier){
        this.identifier=identifier;
    }
    @Override
    public void afterProcess() throws IOException {
        try {
            close();
        } catch (IOException ex) {
            log.error("{}", ex.getMessage());
        }
    }
    public void withFilterSql(String filterSql) {
        this.filterSql = filterSql;
        segment = CommSqlParser.parseSingleTableQuerySql(filterSql, Lex.MYSQL, colmeta, defaultNewColumnPrefix);
        useFilter = true;
        useOrderBy=!CollectionUtils.isEmpty(segment.getOrderBys());
        useGroupBy=!CollectionUtils.isEmpty(segment.getGroupBy());
    }
    protected void groupOrderByInit() throws Exception{
        if(useOrderBy || useGroupBy){
            //pool all record through OffHeap
            //ByteBuffer buffer=ByteBuffer.allocate(512);
            pullNext();
            StringBuilder builder=new StringBuilder();
            while (!CollectionUtils.isEmpty(cachedValue)){
                while (!CollectionUtils.isEmpty(cachedValue) && useFilter && !CommRecordFilter.doesRecordAcceptable(segment, cachedValue)) {
                    pullNext();
                }
                if (segment != null && (!segment.isIncludeAllOriginColumn() && !CollectionUtils.isEmpty(segment.getSelectColumns()))) {
                    newRecord.clear();
                    CommRecordFilter.doAsyncCalculator(segment, cachedValue, newRecord);
                }
                //get group by column
                if(!CollectionUtils.isEmpty(segment.getGroupBy())){
                    if(builder.length()>0){
                        builder.delete(0,builder.length());
                    }
                    for(SqlNode tnode:segment.getGroupBy()) {
                        String columnName=((SqlIdentifier)tnode).getSimple();
                        if (!ObjectUtils.isEmpty(newRecord.get(columnName))) {
                            CommRecordFilter.appendByType(builder,newRecord.get(columnName));
                        }
                    }
                    CommRecordFilter.doGroupAgg(builder.toString(),segment,cachedValue,newRecord,groupByMap);//ByteBufferUtils.getContent(buffer)
                }
                pullNext();
            }
            //calculate avg
            for(CommSqlParser.ValueParts parts:segment.getSelectColumns()){
                if("avg".equalsIgnoreCase(parts.getFunctionName())){
                    groupByMap.entrySet().forEach(entry->{
                        if(!ObjectUtils.isEmpty(entry.getValue().get(parts.getAliasName())) &&
                                !ObjectUtils.isEmpty(entry.getValue().get(parts.getAliasName()+"cou"))){
                            entry.getValue().put(parts.getAliasName(),(Double)entry.getValue().get(parts.getAliasName())/(Integer)entry.getValue().get(parts.getAliasName()+"cou"));
                            entry.getValue().remove(parts.getAliasName()+"cou");
                        }
                    });
                }
            }
            groupIter=groupByMap.entrySet().iterator();
        }
    }
    private String getHavingColumnName(){
        String aliasName=null;
        if(!CollectionUtils.isEmpty(segment.getHaving())){
            for(CommSqlParser.ValueParts parts:segment.getSelectColumns()){
                if(!ObjectUtils.isEmpty(parts.getFunctionName()) && parts.getFunctionName().equals(segment.getHaving().get(0).getFunctionName()) && parts.getCalculator().equals(segment.getHaving().get(0).getCalculator())){
                    aliasName= parts.getAliasName();
                    break;
                }
            }
        }
        return aliasName;
    }
}
