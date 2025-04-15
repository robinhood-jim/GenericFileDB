package com.robin.gfdb.record.writer;

import com.robin.core.base.util.Const;
import com.robin.core.fileaccess.meta.DataCollectionMeta;
import com.robin.gfdb.storage.AbstractFileSystem;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;

import java.io.IOException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.Map;

public class AvroFileWriter extends AbstractFileWriter{
    private Schema schema;
    private DatumWriter<GenericRecord> dwriter;
    private DataFileWriter<GenericRecord> fileWriter;

    protected AvroFileWriter(DataCollectionMeta colmeta, AbstractFileSystem fileSystem) {
        super(colmeta, fileSystem);
        setIdentifier(Const.FILEFORMATSTR.AVRO.getValue());
        useRawOutputStream=true;
    }

    @Override
    public void initalize() throws IOException {
        super.initalize();
        dwriter=new GenericDatumWriter<>(schema);
        fileWriter=new DataFileWriter<>(dwriter);
        Const.CompressType type= getCompressType();
        switch (type){
            case COMPRESS_TYPE_GZ:
                throw new IOException("avro does not support gz compression");
            case COMPRESS_TYPE_BZ2:
                fileWriter.setCodec(CodecFactory.bzip2Codec());
            case COMPRESS_TYPE_LZO:
                throw new IOException("avro does not support lzo compression");
            case COMPRESS_TYPE_SNAPPY:
                fileWriter.setCodec(CodecFactory.snappyCodec());
                break;
            case COMPRESS_TYPE_ZIP:
                fileWriter.setCodec(CodecFactory.deflateCodec(CodecFactory.DEFAULT_DEFLATE_LEVEL));
            case COMPRESS_TYPE_LZ4:
                throw new IOException("avro does not support lz4 compression");
            case COMPRESS_TYPE_LZMA:
                throw new IOException("avro does not support lzma compression");
            case COMPRESS_TYPE_ZSTD:
                throw new IOException("avro does not support zstd compression");
            case COMPRESS_TYPE_BROTLI:
                throw new IOException("avro does not support brotil compression");
            case COMPRESS_TYPE_XZ:
                fileWriter.setCodec(CodecFactory.xzCodec(CodecFactory.DEFAULT_XZ_LEVEL));
                break;
            default:
                fileWriter.setCodec(CodecFactory.nullCodec());
        }
        fileWriter.create(schema,outputStream);
    }

    @Override
    public void writeRecord(Map<String, Object> map) throws IOException {
        GenericRecord grecord=new GenericData.Record(schema);

        for (int i = 0; i < colmeta.getColumnList().size(); i++) {
            String name = colmeta.getColumnList().get(i).getColumnName();
            Object value=getMapValueByMeta(map,name);
            Schema columnSchema=schema.getField(name).schema();

            if(value!=null){
                if(Schema.Type.LONG.equals(columnSchema.getType()) && LogicalTypes.timestampMillis().equals(columnSchema.getLogicalType())){
                    Long ts=0L;
                    if(Timestamp.class.isAssignableFrom(value.getClass())){
                        Timestamp timestamp=(Timestamp)value;
                        ts=timestamp.getTime();
                    }else if(LocalDateTime.class.isAssignableFrom(value.getClass())){
                        LocalDateTime dt=(LocalDateTime)value;
                        ts=dt.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
                    }else if(Date.class.isAssignableFrom(value.getClass())){
                        Date dt=(Date)value;
                        ts=dt.getTime();
                    }
                    grecord.put(name,ts);
                }else{
                    grecord.put(name, value);
                }
            }
        }

        fileWriter.append(grecord);
    }

    @Override
    public void finishWrite() throws IOException {
        fileWriter.flush();
        fileWriter.close();
        outputStream.close();
    }
}
