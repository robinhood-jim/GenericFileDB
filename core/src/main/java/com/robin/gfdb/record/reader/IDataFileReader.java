package com.robin.gfdb.record.reader;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

public interface IDataFileReader extends Closeable,Iterator<Map<String,Object>> {

    //Iterator<Map<String,Object>> getRecords(DataCollectionMeta colmeta,String filePath, AbstractFileSystem fileSystem);
    void init() throws IOException;
    void afterProcess() throws IOException;
    String getIdentifier();
    Map<String,Object> pullNext();

}
