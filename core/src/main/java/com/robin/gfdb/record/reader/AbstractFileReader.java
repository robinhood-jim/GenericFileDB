package com.robin.gfdb.record.reader;

import com.robin.core.base.exception.MissingConfigException;
import com.robin.core.base.util.Const;
import com.robin.core.base.util.FileUtils;
import com.robin.core.base.util.IOUtils;
import com.robin.core.base.util.ResourceConst;
import com.robin.core.fileaccess.meta.DataCollectionMeta;
import com.robin.core.fileaccess.meta.DataSetColumnMeta;
import com.robin.gfdb.sql.filter.CommRecordFilter;
import com.robin.gfdb.sql.parser.CommSqlParser;
import com.robin.gfdb.sql.parser.SqlSegment;
import com.robin.gfdb.storage.AbstractFileSystem;
import com.robin.gfdb.utils.arithmetic.PolandNotationUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;

import java.io.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Slf4j
public abstract class AbstractFileReader implements IDataFileReader{
    protected BufferedReader reader;
    protected String identifier;
    protected DataCollectionMeta colmeta;
    protected AbstractFileSystem fileSystem;
    protected InputStream inputStream;
    protected Map<String, Object> cachedValue = new ConcurrentHashMap<>();
    protected Map<String, Object> newRecord = new ConcurrentHashMap<>();
    protected Map<String, DataSetColumnMeta> columnMap = new HashMap<>();
    protected DateTimeFormatter formatter=DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    //if using BufferedReader as input.only csv format must set this to true
    protected boolean useBufferedReader=false;
    protected boolean useRawInputStream=true;
    protected boolean useOrderBy=false;
    protected boolean useGroupBy=false;
    private  boolean useFilter=false;
    private String filterSql;
    protected SqlSegment segment;
    protected String defaultNewColumnPrefix = "N_COLUMN";
    protected Iterator<Map.Entry<String,Map<String,Object>>> groupIter;
    protected Map<String,Map<String,Object>> groupByMap=new ConcurrentHashMap<>();
    protected List<String> columnNames=new ArrayList<>();
    protected boolean partJob=false;

    public AbstractFileReader(DataCollectionMeta colmeta,AbstractFileSystem fileSystem){
        this.colmeta=colmeta;
        this.fileSystem=fileSystem;
        for (DataSetColumnMeta meta : colmeta.getColumnList()) {
            columnMap.put(meta.getColumnName(), meta);
            if (Const.META_TYPE_FORMULA.equals(meta.getColumnType())) {
                meta.setColumnType(Const.META_TYPE_DOUBLE);
            }
            columnNames.add(meta.getColumnName());
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
                newRecord.clear();
                while (!CollectionUtils.isEmpty(cachedValue) && useFilter && !CommRecordFilter.doesRecordAcceptable(segment, cachedValue,newRecord)) {
                    pullNext();
                    newRecord.clear();
                }
                if(CollectionUtils.isEmpty(cachedValue)){
                    return false;
                }
                if (segment != null && (!segment.isIncludeAllOriginColumn() && !CollectionUtils.isEmpty(segment.getSelectColumns()))) {
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
                    if(!CollectionUtils.isEmpty(segment.getHaving()) && !partJob) {
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

    public void withFilterSql(String filterSql) {
        this.filterSql = filterSql;
        segment = CommSqlParser.parseSingleTableQuerySql(filterSql, Lex.MYSQL, colmeta, defaultNewColumnPrefix);
        useFilter = true;
        useOrderBy=!CollectionUtils.isEmpty(segment.getOrderBys());
        useGroupBy=!CollectionUtils.isEmpty(segment.getGroupBy());
    }
    protected void groupOrderByInit() throws Exception{
        if(useOrderBy || useGroupBy){
            Set<String> existKeys=new HashSet<>();
            //pool all record through OffHeap
            pullNext();

            StringBuilder builder=new StringBuilder();
            while (!CollectionUtils.isEmpty(cachedValue)){
                newRecord.clear();
                while (!CollectionUtils.isEmpty(cachedValue) && useFilter && !CommRecordFilter.doesRecordAcceptable(segment, cachedValue,newRecord)) {
                    pullNext();
                    newRecord.clear();
                }
                if (segment != null && (!segment.isIncludeAllOriginColumn() && !CollectionUtils.isEmpty(segment.getSelectColumns()))) {
                    if(!CollectionUtils.isEmpty(cachedValue)) {
                        CommRecordFilter.doAsyncCalculator(segment, cachedValue, newRecord);
                        if(existKeys.isEmpty()){
                            existKeys.addAll(newRecord.keySet());
                        }
                    }
                }
                //get group by column
                if(!CollectionUtils.isEmpty(segment.getGroupBy()) && !CollectionUtils.isEmpty(newRecord)){
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
            final List<String> outPutColumns=segment.getSelectColumns().stream().map(f->!ObjectUtils.isEmpty(f.getAliasName())?f.getAliasName():f.getIdentifyColumn()).collect(Collectors.toList());
            log.debug("outputColumns :"+outPutColumns);
            existKeys.removeAll(outPutColumns);
            log.debug("remove column "+existKeys);
            List<String> avgColumns=new ArrayList<>();
            for(CommSqlParser.ValueParts parts:segment.getSelectColumns()){
                if("avg".equalsIgnoreCase(parts.getFunctionName()) && !partJob){
                    avgColumns.add(parts.getAliasName());
                }
            }
            if(!CollectionUtils.isEmpty(avgColumns) || !CollectionUtils.isEmpty(existKeys)) {
                Iterator<Map.Entry<String,Map<String,Object>>> iterator=groupByMap.entrySet().iterator();
                while(iterator.hasNext()){
                    Map.Entry<String,Map<String,Object>> entry=iterator.next();
                    if(!CollectionUtils.isEmpty(avgColumns)) {
                        for(int i=0;i<avgColumns.size();i++) {
                            if (!ObjectUtils.isEmpty(entry.getValue().get(avgColumns.get(i))) &&
                                    !ObjectUtils.isEmpty(entry.getValue().get(avgColumns.get(i) + "cou"))) {
                                entry.getValue().put(avgColumns.get(i), (Double) entry.getValue().get(avgColumns.get(i)) / (Integer) entry.getValue().get(avgColumns.get(i) + "cou"));
                                entry.getValue().remove(avgColumns.get(i) + "cou");
                            }
                        }
                    }
                    if(!CollectionUtils.isEmpty(existKeys)){
                        for(String removeKey:existKeys){
                            entry.getValue().remove(removeKey);
                        }
                    }
                }
            }
            groupIter=groupByMap.entrySet().iterator();
            log.debug("resultMap {}",groupByMap);
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
    protected void copyToLocal(File tmpFile, InputStream stream) {
        try (FileOutputStream outputStream = new FileOutputStream(tmpFile)) {
            IOUtils.copyBytes(stream, outputStream, 8192);
        } catch (IOException ex) {
            log.error("{}", ex.getMessage());
        }
    }
    public void setDateTimeFormat(String formatStr){
        formatter=DateTimeFormatter.ofPattern(formatStr);
    }


    public Map<String, DataSetColumnMeta> getColumnMap() {
        return columnMap;
    }

    public SqlSegment getSegment() {
        return segment;
    }

    public List<String> getColumnNames() {
        return columnNames;
    }
    public List<DataSetColumnMeta> getCalculatedSchema(){
        if(!useFilter){
            return colmeta.getColumnList();
        }else{
            return segment.getCalculateSchema();
        }
    }
    protected Const.CompressType getCompressType(){
        if(ObjectUtils.isEmpty(colmeta.getContent())) {
            FileUtils.FileContent content = FileUtils.parseFile(colmeta.getPath());
            colmeta.setContent(content);
        }
        return colmeta.getContent().getCompressType();
    }
    public void setPartJob(){
        partJob=true;
    }

}
