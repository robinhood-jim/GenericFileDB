package com.robin.gfdb.record.reader;

import com.robin.core.base.exception.MissingConfigException;
import com.robin.core.base.util.FileUtils;
import com.robin.core.fileaccess.meta.DataCollectionMeta;
import com.robin.gfdb.storage.AbstractFileSystem;
import org.springframework.util.Assert;

import java.io.IOException;

public class FileReaderFactory {
    public static AbstractFileReader getReader(DataCollectionMeta collectionMeta, AbstractFileSystem fileSystem, String path) throws IOException {
        Assert.notNull(fileSystem,"fileSystem must not be null");
        String processFile=path;
        if(fileSystem.isDirectory(path)){
            processFile=fileSystem.listOne(path);
        }
        if(collectionMeta.getContent()==null){
            collectionMeta.setContent(FileUtils.parseFile(processFile));
        }
        AbstractFileReader reader=null;
        switch (collectionMeta.getContent().getFileFormat()){
            case CSV:
                reader=new CsvFileReader(collectionMeta,fileSystem);
                break;
            case XLSX:
                reader=new XlsxFileReader(collectionMeta,fileSystem);
                break;
            case XML:
                reader=new XmlFileReader(collectionMeta,fileSystem);
                break;
            case ORC:
                reader=new OrcFileReader(collectionMeta,fileSystem);
                break;
            case AVRO:
                reader=new AvroFileReader(collectionMeta,fileSystem);
                break;
            case JSON:
                reader=new JsonFileReader(collectionMeta,fileSystem);
                break;
            case PARQUET:
                reader=new ParquetFileReader(collectionMeta,fileSystem);
                break;
            case PROTOBUF:
                reader =new ProtoBufFileReader(collectionMeta,fileSystem);
                break;
            default:
                throw new MissingConfigException("type "+collectionMeta.getContent().getFileFormat().getValue()+" not supported!");
        }
        return reader;

    }
}
