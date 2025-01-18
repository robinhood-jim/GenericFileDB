package com.robin.gfdb.record.reader;

import com.google.gson.stream.JsonReader;
import com.robin.core.base.util.Const;
import com.robin.core.convert.util.ConvertUtil;
import com.robin.core.fileaccess.meta.DataCollectionMeta;
import com.robin.core.fileaccess.meta.DataSetColumnMeta;
import com.robin.gfdb.storage.AbstractFileSystem;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Map;

@Slf4j
public class JsonFileReader extends AbstractFileReader{
    private JsonReader jreader=null;
    public JsonFileReader(DataCollectionMeta colmeta, AbstractFileSystem fileSystem) {
        super(colmeta, fileSystem);
        setIdentifier(Const.FILEFORMATSTR.JSON.getValue());
    }

    @Override
    public void init() throws IOException {
        super.init();

    }

    @Override
    public Map<String, Object> pullNext() {
        DataSetColumnMeta meta=null;
        try{
            cachedValue.clear();
            if(jreader.hasNext()){
                jreader.beginObject();
                while(jreader.hasNext()){
                    String column = jreader.nextName();
                    String value = jreader.nextString();
                    if(!columnMap.containsKey(column)){
                        if(columnMap.containsKey(column.toLowerCase())){
                            column=column.toLowerCase();
                        }else if(columnMap.containsKey(column.toUpperCase())){
                            column=column.toUpperCase();
                        }
                    }
                    meta=columnMap.get(column);
                    cachedValue.put(column, ConvertUtil.convertStringToTargetObject(value, meta, formatter));
                }
                jreader.endObject();
            }
        }catch(IOException ex){
            log.error("{}",ex.getMessage());
        }catch (Exception e) {
            log.error("{}",e.getMessage());
        }
        return cachedValue;
    }


}
