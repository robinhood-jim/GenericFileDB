GenericFileDB
=========
[![Build Status](https://github.com/robinhood-jim/GenericFileDB/actions/workflows/maven.yml/badge.svg?branch=main)](https://github.com/robinhood-jim/GenericFileDB/actions)
![license](https://img.shields.io/badge/license-Apache--2.0-green.svg)
<br>
![structure](https://github.com/robinhood-jim/GenericFileDB/blob/develop/resources/structure.png?raw=type)

<br>
Common Generic DataFile DB  V1.0
Aim to ingest kind of unstructured or half structure source (format including csv/json/xml/arvo/orc/parquet/protobuf/apache arrow) 
and add SQL Capacity and ETL Capacity without flush datas to any Database or hadoop filesystem.
Data file can ingest from local/hdfs/ApacheVfs/AWS s3/google cloud storage/minio/Aliyun/tencent cos/baidu BOS/huawei OBS and etc.
Files less than 4G bytes can process without flush to tmp path. large than 4G orc/parquet/arrow binary file must be download first.
Now only support one file SQL filtering,later will support multiple files with MapReduce 

## Prerequisites
- Java 11+ above.
- Maven 3.8.6 above
- add following to you pom

```xml
<dependency>
    <groupId>com.robin.gfdb</groupId>
    <artifactId>core</artifactId>
    <version>1.0-SNAPSHOT</version>
</dependency>
```

## Examples

### read csv from FileSystemAccessor
```java
    DataCollectionMeta.Builder builder=new DataCollectionMeta.Builder();
    builder.addColumn("id", Const.META_TYPE_BIGINT,null);
    builder.addColumn("name",Const.META_TYPE_STRING,null);
    builder.addColumn("description",Const.META_TYPE_STRING,null);
        ......
    try(LocalFileSystem fileSystem=LocalFileSystem.getInstance();
        AbstractFileReader reader=new CsvFileReader(meta,fileSystem)){
        fileSystem.init(meta);
        reader.init();
        while(reader.hasNext()){
            outputMap=reader.next();
            log.info("{}",outputMap);
        }finally {
            CommRecordFilter.close();
        }
        
```
        
	
	