package com.robin.gfdb.record.writer;

import com.robin.core.base.datameta.DataBaseUtil;
import com.robin.core.base.util.Const;
import com.robin.core.base.util.FileUtils;
import com.robin.core.fileaccess.meta.DataCollectionMeta;
import com.robin.core.fileaccess.meta.DataSetColumnMeta;
import com.robin.gfdb.storage.AbstractFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.ObjectUtils;
import org.tukaani.xz.FinishableOutputStream;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.time.format.DateTimeFormatter;
import java.util.*;

public abstract class AbstractFileWriter implements IDataFileWriter {
    protected BufferedWriter writer;
    protected DataCollectionMeta colmeta;
    protected OutputStream outputStream;
    protected Map<String, String> columnMap=new HashMap<>();
    protected List<String> columnList=new ArrayList<>();
    protected Logger logger= LoggerFactory.getLogger(getClass());
    protected DateTimeFormatter formatter;
    protected AbstractFileSystem fileSystem;
    protected String identifier;
    protected boolean useBufferedWriter =false;
    protected boolean useRawOutputStream=false;
    protected AbstractFileWriter(DataCollectionMeta colmeta,AbstractFileSystem fileSystem){
        this.colmeta=colmeta;
        formatter=DateTimeFormatter.ofPattern(colmeta.getDefaultTimestampFormat());
        for (DataSetColumnMeta meta:colmeta.getColumnList()) {
            columnList.add(meta.getColumnName());
            columnMap.put(meta.getColumnName(), meta.getColumnType());
        }
        this.fileSystem=fileSystem;
    }
    public void setWriter(BufferedWriter writer){
        this.writer=writer;
    }
    public void setOutputStream(OutputStream outputStream){
        this.outputStream=outputStream;
    }

    @Override
    public void initalize() throws IOException {
        if(outputStream==null){
            if(!useRawOutputStream) {
                outputStream = fileSystem.getOutResourceByStream(colmeta, colmeta.getPath());
            }else {
                outputStream=fileSystem.getRawOutputStream(colmeta,colmeta.getPath());
            }
            if(useBufferedWriter) {
                writer = new BufferedWriter(new OutputStreamWriter(outputStream));
            }
        }
        logger.info("using Writer {}",getClass().getCanonicalName());
    }
    @Override
    public void writeRecord(List<Object> map) throws IOException {
        writeRecord(wrapListToMap(map));
    }

    protected Map<String, Object> wrapListToMap(List<Object> list){
        Map<String, Object> valuemap=new HashMap<>();
        if(list.size()<colmeta.getColumnList().size()) {
            return Collections.emptyMap();
        }
        for (int i=0;i<colmeta.getColumnList().size();i++) {
            DataSetColumnMeta meta = colmeta.getColumnList().get(i);
            valuemap.put(meta.getColumnName(), list.get(i));
        }
        return valuemap;
    }
    @Override
    public void close() throws IOException{
        doClose();
    }
    protected void doClose() throws IOException{
        if(writer!=null){
            writer.close();
        }
        fileSystem.finishWrite(colmeta,outputStream);
        if(outputStream!=null){
            outputStream.close();
        }
    }
    protected Const.CompressType getCompressType(){
        if(ObjectUtils.isEmpty(colmeta.getContent())) {
            FileUtils.FileContent content = FileUtils.parseFile(colmeta.getPath());
            colmeta.setContent(content);
        }
        return colmeta.getContent().getCompressType();
    }
    protected String getOutputStringByType(Map<String,Object> valueMap,String columnName){
        String columnType=columnMap.get(columnName);
        Object obj=getMapValueByMeta(valueMap,columnName);
        if(obj!=null) {
            return DataBaseUtil.toStringByDBType(obj,columnType,formatter);
        } else{
            return null;
        }
    }
    protected Object getMapValueByMeta(Map<String,?> valueMap,String columnName){
        Object obj=null;
        String columnType=columnMap.get(columnName);
        if(valueMap.containsKey(columnName)){
            obj=valueMap.get(columnName);
        }else if(valueMap.containsKey(columnName.toUpperCase())){
            obj=valueMap.get(columnName.toUpperCase());
        }else if(valueMap.containsKey(columnName.toLowerCase())){
            obj=valueMap.get(columnName.toLowerCase());
        }
        if(DataBaseUtil.isValueValid(obj,columnType)) {
            return obj;
        } else {
            return null;
        }
    }

    @Override
    public void flush() throws IOException {
        doFlush();
    }
    protected void doFlush(){
        try {
            if(!FinishableOutputStream.class.isAssignableFrom(outputStream.getClass())) {
                if(writer!=null) {
                    writer.flush();
                }
                outputStream.flush();
            }
        }catch (Exception ex){

        }
    }
    @Override
    public String getIdentifier() {
        return identifier;
    }
    protected void setIdentifier(String identifier){
        this.identifier=identifier;
    }
}
