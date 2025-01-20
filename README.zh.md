#
通用类型数据文件SQL支持  
旨在对不同格式的数据文件的基于标准sql的能力支持，支持数据格式包含（csv/json/xml/arvo/orc/parquet/protobuf/Apache Arrow），支持大数据量文件直接读取，
目前支持单表的组合条件查询以及group by having等（全局order by目前未支持），数据文件直接读取，不转储到任意数据库和大数据的hadoop环境，
且数据文件支持文件系统包含（本地文件/hdfs/ApacheVfs/AWS s3/google云/minio/Aliyun/tencent cos/baidu BOS/huawei OBS等），
小于4G的数据文件可以不落地直接处理，大于4G以上的parquet/avro/apache arrow需要落地处理。


![license](https://img.shields.io/badge/license-Apache--2.0-green.svg)

开发环境
    JDK 11以上，maven 3.8