package com.robin.gfdb.record.reader;

import com.robin.core.base.util.Const;
import com.robin.core.fileaccess.meta.DataCollectionMeta;
import com.robin.core.fileaccess.meta.DataSetColumnMeta;
import com.robin.gfdb.storage.AbstractFileSystem;
import com.robin.gfdb.storage.ApacheVfsFileSystem;
import com.robin.gfdb.utils.arithmetic.PolandNotationUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public abstract class AbstractFileReader implements IDataFileReader{
    protected BufferedReader reader;
    protected String identifier;
    protected DataCollectionMeta colmeta;
    protected AbstractFileSystem fileSystem;
    protected InputStream inputStream;
    protected Map<String, Object> cachedValue = new HashMap<>();
    protected Map<String, DataSetColumnMeta> columnMap = new HashMap<>();
    protected DateTimeFormatter formatter=DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    //if using BufferedReader as input.only csv json format must set this to true
    protected boolean useBufferedReader=false;
    protected boolean useRawInputStream=false;
    public AbstractFileReader(DataCollectionMeta colmeta,AbstractFileSystem fileSystem){
        this.colmeta=colmeta;
        this.fileSystem=fileSystem;
        for (DataSetColumnMeta meta : colmeta.getColumnList()) {
            columnMap.put(meta.getColumnName(), meta);
            if (Const.META_TYPE_FORMULA.equals(meta.getColumnType())) {
                meta.setColumnType(Const.META_TYPE_DOUBLE);
            }
        }
    }

    @Override
    public void init() throws IOException {
        Assert.notNull(fileSystem,"FileSystem is missing");
        if(useBufferedReader){
            Pair<BufferedReader, InputStream> pair = fileSystem.getInResourceByReader(colmeta, colmeta.getPath());
            this.reader = pair.getKey();
            this.inputStream = pair.getValue();
        }else{
            if(!useRawInputStream) {
                this.inputStream = fileSystem.getInResourceByStream(colmeta, colmeta.getPath());
            }else{
                this.inputStream=fileSystem.getRawInputStream(colmeta,colmeta.getPath());
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
        pullNext();
        return !CollectionUtils.isEmpty(cachedValue);
    }

    @Override
    public Map<String, Object> next() {
        return cachedValue;
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
}
