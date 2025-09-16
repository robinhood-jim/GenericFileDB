package com.robin.gfdb.record.reader;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.robin.core.base.exception.OperationNotSupportException;
import com.robin.core.base.util.Const;
import com.robin.core.base.util.ResourceConst;
import com.robin.core.fileaccess.meta.DataCollectionMeta;
import com.robin.core.fileaccess.util.AvroUtils;
import com.robin.core.fileaccess.util.ResourceUtil;
import com.robin.gfdb.hdfs.HDFSUtil;
import com.robin.gfdb.record.utils.*;
import com.robin.gfdb.storage.AbstractFileSystem;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.calcite.sql.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.proto.ProtoParquetReader;
import org.apache.parquet.proto.ProtoReadSupport;
import org.apache.parquet.schema.MessageType;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

import static org.apache.parquet.filter2.predicate.FilterApi.and;
import static org.apache.parquet.filter2.predicate.FilterApi.or;

public class ParquetFileReader extends AbstractFileReader implements IDataFileReader{
    private ParquetReader<GenericData.Record> preader;
    private ParquetReader<DynamicMessage.Builder> protoReader;
    ParquetReader<Map<String, Object>> ireader;
    private Schema schema;
    private MessageType msgtype;
    private ProtoBufUtil.ProtoContainer container;
    private boolean useAvroEncode = false;
    private boolean useProtoBuffEncode=false;
    private HDFSUtil hdfsUtil;
    private Configuration conf;
    InputFile file;
    FilterCompat.Filter filter;
    GenericData.Record record;
    DynamicMessage message;


    @Override
    public void init() throws IOException {
        super.init();
        if (colmeta.getResourceCfgMap().containsKey(ResourceConst.PARQUETFILEFORMAT)) {
            if(ResourceConst.PARQUETSUPPORTFORMAT.AVRO.getValue().equalsIgnoreCase(colmeta.getResourceCfgMap().get(ResourceConst.PARQUETFILEFORMAT).toString())) {
                useAvroEncode = true;
            }else if(ResourceConst.PARQUETSUPPORTFORMAT.PROTOBUF.getValue().equalsIgnoreCase(colmeta.getResourceCfgMap().get(ResourceConst.PARQUETFILEFORMAT).toString())){
                useProtoBuffEncode=true;
            }
        }
        if (!ObjectUtils.isEmpty(super.segment) && !super.segment.isConditionHasFourOperations() && !super.segment.isHasRightColumnCmp()) {
            FilterPredicate predicate=walkCondition(super.segment.getWhereCause());
            filter= FilterCompat.get(predicate);
        }
        parseSchema();
        if (Const.FILESYSTEM.HDFS.getValue().equals(colmeta.getFsType())) {
            conf = new HDFSUtil(colmeta).getConfig();
            file = HadoopInputFile.fromPath(new Path(colmeta.getPath()), conf);

            if (useAvroEncode) {
                ParquetReader.Builder<GenericData.Record>  avroBuilder= AvroParquetReader
                        .<GenericData.Record>builder(HadoopInputFile.fromPath(new Path(ResourceUtil.getProcessPath(colmeta.getPath())), conf)).withConf(conf);
                if(filter==null) {
                    avroBuilder.withFilter(filter);
                }
                preader=avroBuilder.build();
            }else if (useProtoBuffEncode){
                ParquetReader.Builder<DynamicMessage.Builder> protoBuilder= ProtoParquetReader.<DynamicMessage.Builder>builder(HadoopInputFile.fromPath(new Path(ResourceUtil.getProcessPath(colmeta.getPath())), conf))
                        .set(ProtoReadSupport.PB_CLASS,DynamicMessage.class.getName()).withConf(conf);
                if(filter==null) {
                    protoBuilder.withFilter(filter);
                }
                protoReader=protoBuilder.build();
            }else{
                ParquetReader.Builder<Map<String, Object>> builder = ParquetReader.builder(new CustomRowReadSupport(msgtype), new Path(ResourceUtil.getProcessPath(colmeta.getPath()))).withConf(conf);
                if(filter==null) {
                    builder.withFilter(filter);
                }
                ireader = builder.build();
            }
        }else if(Const.FILESYSTEM.LOCAL.getValue().equals(colmeta.getFsType())){

        }

    }
    private void parseSchema(){
        Assert.notNull(file,"");
        Assert.isTrue(!CollectionUtils.isEmpty(colmeta.getColumnList()),"");
        if(useAvroEncode){
            schema = AvroUtils.getSchemaFromMeta(colmeta);
        }else if(useProtoBuffEncode){
            container=ProtoBufUtil.initSchema(colmeta);
        }else{
            msgtype= ParquetUtil.genSchema(colmeta);
        }
    }
    private FilterPredicate walkCondition(SqlNode node) {
        //condition has four operation or function call column,FilterPredicate can not perform,otherwise can use
        if (SqlBasicCall.class.isAssignableFrom(node.getClass())) {
            List<SqlNode> nodes = ((SqlBasicCall) node).getOperandList();
            if (SqlIdentifier.class.isAssignableFrom(nodes.get(0).getClass()) && SqlLiteral.class.isAssignableFrom(nodes.get(1).getClass())) {
                return ParquetReaderUtil.parseOperator(this,nodes.get(0));
            } else {
                FilterPredicate left = walkCondition(nodes.get(0));
                FilterPredicate right = walkCondition(nodes.get(1));
                if(left==null){
                    left= FilterApi.userDefined(FilterApi.intColumn("a"),new YesPredicate());
                }
                if(right==null){
                    right= FilterApi.userDefined(FilterApi.intColumn("a"),new YesPredicate());
                }
                if (SqlKind.OR.equals(node.getKind())) {
                    return or(left, right);
                } else {
                    return and(left, right);
                }
            }
        } else if(SqlIdentifier.class.isAssignableFrom(node.getClass()) && SqlLiteral.class.isAssignableFrom(node.getClass())){
            return ParquetReaderUtil.parseOperator(this,node);
        }else{
            return null;
        }
    }

    public ParquetFileReader(DataCollectionMeta colmeta, AbstractFileSystem fileSystem) {
        super(colmeta, fileSystem);
        setIdentifier(Const.FILEFORMATSTR.PARQUET.getValue());
        useRawInputStream=true;
    }

    @Override
    public Map<String, Object> pullNext() {
        try{
            cachedValue.clear();
            if (useAvroEncode) {
                record = preader.read();
                if(record!=null){
                    for (Schema.Field field : schema.getFields()) {
                        Object value = record.get(field.name());
                        if (LogicalTypes.timestampMillis().equals(field.schema().getLogicalType())) {
                            value = new Timestamp((Long) value);
                        }
                        cachedValue.put(field.name(), value);
                    }
                }
            }else if(useProtoBuffEncode){
                message=protoReader.read().build();
                if(message!=null){
                    for (Descriptors.FieldDescriptor descriptor : container.getSchema().getMessageDescriptor(colmeta.getValueClassName()).getFields()) {
                        cachedValue.put(descriptor.getName(), message.getField(descriptor));
                    }
                }
            }else{
                cachedValue = ireader.read();
            }
        }catch (IOException ex){
            throw new OperationNotSupportException(ex);
        }
        return cachedValue;
    }
}
