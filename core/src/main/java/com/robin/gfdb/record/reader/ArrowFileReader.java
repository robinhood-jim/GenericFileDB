package com.robin.gfdb.record.reader;

import com.robin.core.base.exception.MissingConfigException;
import com.robin.core.base.util.Const;
import com.robin.core.fileaccess.meta.DataCollectionMeta;
import com.robin.gfdb.record.utils.ArrowSchemaUtils;
import com.robin.gfdb.storage.AbstractFileSystem;
import org.apache.arrow.compression.CommonsCompressionFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.compression.CompressionCodec;
import org.apache.arrow.vector.compression.NoCompressionCodec;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.springframework.util.CollectionUtils;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

public class ArrowFileReader extends AbstractFileReader{
    private Schema schema;
    private ArrowStreamReader streamReader;
    private BufferAllocator allocator;
    private VectorSchemaRoot vectorSchemaRoot;
    private int maxrows;
    private int currentbatchRow;
    private VectorSchemaRoot currentBatch;
    private long curpos=0L;
    public ArrowFileReader(DataCollectionMeta colmeta, AbstractFileSystem fileSystem) {
        super(colmeta,fileSystem);
        setIdentifier(Const.FILEFORMATSTR.ARROW.getValue());
        useRawInputStream=true;
    }

    @Override
    public void init() throws IOException {
        super.init();
        schema= ArrowSchemaUtils.getSchema(colmeta);

        allocator=new RootAllocator(Integer.MAX_VALUE);
        vectorSchemaRoot=VectorSchemaRoot.create(schema,allocator);
        CompressionCodec.Factory factory= new NoCompressionCodec.Factory();
        Const.CompressType type= getCompressType();
        switch (type){
            case COMPRESS_TYPE_LZ4:
            case COMPRESS_TYPE_ZSTD:
                factory=new CommonsCompressionFactory();
                break;
            default:
                throw new MissingConfigException("not supported");
        }
        streamReader=new ArrowStreamReader(inputStream,allocator,factory);
    }

    @Override
    public Map<String, Object> pullNext() {
        boolean hasRec=false;
        try {
            cachedValue.clear();
            if((currentbatchRow==0|| maxrows==0) || (currentbatchRow>0 && currentbatchRow>=maxrows)) {
                hasRec = streamReader.loadNextBatch();
                currentbatchRow=0;
                if(!hasRec){
                    return null;
                }
                currentBatch= streamReader.getVectorSchemaRoot();
                maxrows=currentBatch.getRowCount();
            }
            List<Field> fields=schema.getFields();
            if(!CollectionUtils.isEmpty(fields)){
                for(int i=0;i<fields.size();i++){
                    wrapValue(fields.get(i),fields.get(i).getName(),colmeta.getColumnList().get(i).getColumnType(),currentbatchRow,cachedValue);
                }
            }
            currentbatchRow++;
            curpos++;
        }catch (Exception ex){
            ex.printStackTrace();
        }
        return null;
    }
    public void wrapValue(Field field, String columnName, String columnType, int row, Map<String,Object> valueMap) throws UnsupportedEncodingException {
        FieldVector vectors=currentBatch.getVector(field);
        if(IntVector.class.isAssignableFrom(vectors.getClass())){
            IntVector intVector=(IntVector) vectors;
            if(Const.META_TYPE_INTEGER.equals(columnType)){
                valueMap.put(columnName,intVector.get(row));
            }else if(Const.META_TYPE_BIGINT.equals(columnType)){
                valueMap.put(columnName,intVector.getValueAsLong(row));
            }
        }else if(BigIntVector.class.isAssignableFrom(vectors.getClass())){
            BigIntVector bVector=(BigIntVector) vectors;
            if(Const.META_TYPE_INTEGER.equals(columnType)){
                valueMap.put(columnName,Integer.valueOf(String.valueOf(bVector.get(row))));
            }else if(Const.META_TYPE_BIGINT.equals(columnType)){
                valueMap.put(columnName,bVector.getValueAsLong(row));
            }
        }else if(TimeStampVector.class.isAssignableFrom(vectors.getClass())){
            valueMap.put(columnName,new Timestamp(((TimeStampVector)vectors).get(row)));
        }else if(Const.META_TYPE_STRING.equals(columnType)){
            valueMap.put(columnName,new String(((VarCharVector)vectors).get(row),"utf-8"));
        }
    }
}
