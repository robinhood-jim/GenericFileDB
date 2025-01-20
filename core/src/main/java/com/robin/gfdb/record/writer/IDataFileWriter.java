package com.robin.gfdb.record.writer;


import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface IDataFileWriter extends Closeable {
    void writeRecord(Map<String,Object> map) throws IOException;
    void writeRecord(List<Object> seq) throws IOException;

    void initalize() throws IOException;
    String getIdentifier();
    void finishWrite() throws IOException;
    void flush() throws IOException;
}
