package com.robin.gfdb.record.writer;

import com.google.gson.stream.JsonWriter;
import com.robin.core.base.util.Const;
import com.robin.core.fileaccess.meta.DataCollectionMeta;
import com.robin.gfdb.storage.AbstractFileSystem;
import org.tukaani.xz.FinishableOutputStream;

import java.io.IOException;
import java.util.Map;

public class JsonFileWriter extends AbstractFileWriter implements IDataFileWriter{
    private JsonWriter jwriter=null;
    protected JsonFileWriter(DataCollectionMeta colmeta, AbstractFileSystem fileSystem) {
        super(colmeta, fileSystem);
        setIdentifier(Const.FILEFORMATSTR.JSON.getValue());
        useBufferedWriter=true;
    }

    @Override
    public void initalize() throws IOException {
        super.initalize();
        jwriter=new JsonWriter(writer);
        jwriter.beginArray();
    }

    @Override
    public void writeRecord(Map<String, Object> map) throws IOException {
        try{
            jwriter.beginObject();
            for (int i = 0; i < colmeta.getColumnList().size(); i++) {
                String name = colmeta.getColumnList().get(i).getColumnName();
                String value=getOutputStringByType(map,name);
                if(value!=null){
                    jwriter.name(name).value(value);
                }
            }
            jwriter.endObject();
        }catch(Exception ex){
            logger.error("",ex);
        }
    }

    public void finishWrite() throws IOException {
        jwriter.endArray();
        jwriter.close();
    }

    @Override
    public void flush() throws IOException {
        try {
            if(!FinishableOutputStream.class.isAssignableFrom(outputStream.getClass())) {
                jwriter.flush();
                outputStream.flush();
            }
        }catch (Exception ex){

        }
    }

    @Override
    public void close() throws IOException {
        if(jwriter!=null){
            jwriter.close();
        }
        doClose();
    }
}
