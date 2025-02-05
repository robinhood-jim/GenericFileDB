#
Common Generic DataFile DB  V1.0
Aim to ingest kind of dataFile (file format including csv/json/xml/arvo/orc/parquet/protobuf/apache arrow) 
and Filter/group and order those kind of data using plain sql without flush datas to any Database or hadoop filesystem.
Data file can ingest from local/hdfs/ApacheVfs/AWS s3/google cloud storage/minio/Aliyun/tencent cos/baidu BOS/huawei OBS and etc.
Files less than 4G bytes can process without flush to tmp path. large than 4G orc/parquet/arrow binary file must be download first.


![license](https://img.shields.io/badge/license-Apache--2.0-green.svg)

        Develop Environment
                JDK 11 above
                Maven 3.8 above
        
	
	