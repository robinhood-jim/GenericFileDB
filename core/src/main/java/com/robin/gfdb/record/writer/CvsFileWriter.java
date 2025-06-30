package com.robin.gfdb.record.writer;

import com.robin.core.base.util.Const;
import com.robin.core.fileaccess.meta.DataCollectionMeta;
import com.robin.gfdb.storage.AbstractFileSystem;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CvsFileWriter extends AbstractFileWriter implements IDataFileWriter{
    private List<String> retList=null;
    private String split=",";

    protected CvsFileWriter(DataCollectionMeta colmeta, AbstractFileSystem fileSystem) {
        super(colmeta, fileSystem);
        setIdentifier(Const.FILEFORMATSTR.CSV.getValue());
        useBufferedWriter =true;
    }

    @Override
    public void writeRecord(Map<String, Object> map) throws IOException {
        retList.clear();
        for (int i = 0; i < colmeta.getColumnList().size(); i++) {
            String name=colmeta.getColumnList().get(i).getColumnName();
            String value=getOutputStringByType(map,name);
            if(value!=null){
                retList.add(value);
            }else {
                retList.add("");
            }
        }
        writer.write(StringUtils.join(retList, split)+"\n");
    }

    @Override
    public void initalize() throws IOException {
        super.initalize();
        retList=new ArrayList<>();
    }

    @Override
    public void finishWrite() throws IOException {
        writer.flush();
        writer.close();
    }


    public void setSplit(String split) {
        this.split = split;
    }
}
