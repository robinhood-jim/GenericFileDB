package com.robin.gfdb.record.writer;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.robin.core.base.util.Const;
import com.robin.core.fileaccess.meta.DataCollectionMeta;
import com.robin.gfdb.record.utils.ProtoBufUtil;
import com.robin.gfdb.storage.AbstractFileSystem;
import org.springframework.util.ObjectUtils;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

public class ProtoBufFileWriter extends AbstractFileWriter implements IDataFileWriter{
    private ProtoBufUtil.ProtoContainer container;

    protected ProtoBufFileWriter(DataCollectionMeta colmeta, AbstractFileSystem fileSystem) {
        super(colmeta, fileSystem);
        setIdentifier(Const.FILEFORMATSTR.PROTOBUF.getValue());
        useRawOutputStream=true;
    }

    @Override
    public void initalize() throws IOException {
        super.initalize();
        try {
            container=ProtoBufUtil.initSchema(colmeta);

            getCompressType();
        }catch (Exception ex){
            ex.printStackTrace();
        }
    }

    @Override
    public void writeRecord(Map<String, Object> map) throws IOException {
        Iterator<Map.Entry<String,Object>> iter=map.entrySet().iterator();
        container.getMesgBuilder().clear();
        while(iter.hasNext()){
            Map.Entry<String,Object> entry=iter.next();
            Descriptors.FieldDescriptor des=container.getMsgDesc().findFieldByName(entry.getKey());
            if(des!=null && !ObjectUtils.isEmpty(entry.getValue())){
                container.getMesgBuilder().setField(des,entry.getValue());
            }
        }
        DynamicMessage message=container.getMesgBuilder().build();
        message.writeDelimitedTo(outputStream);
    }

    @Override
    public void finishWrite() throws IOException {

    }
}
