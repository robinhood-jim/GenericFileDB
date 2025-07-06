package com.robin.gfdb.record.reader;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.robin.core.base.util.Const;
import com.robin.core.fileaccess.meta.DataCollectionMeta;
import com.robin.gfdb.record.utils.ProtoBufUtil;
import com.robin.gfdb.storage.AbstractFileSystem;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Map;
import java.util.NoSuchElementException;

@Slf4j
public class ProtoBufFileReader extends AbstractFileReader implements IDataFileReader {
    private ProtoBufUtil.ProtoContainer container;
    private DynamicMessage message;

    public ProtoBufFileReader(DataCollectionMeta colmeta, AbstractFileSystem fileSystem) {
        super(colmeta, fileSystem);
        setIdentifier(Const.FILEFORMATSTR.PROTOBUF.getValue());
    }

    @Override
    public void init() throws IOException {
        super.init();
        try {
            container=ProtoBufUtil.initSchema(colmeta);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @Override
    public Map<String, Object> pullNext() {
        boolean errExist=false;
        try {
            cachedValue.clear();
            if (container.getMesgBuilder().mergeDelimitedFrom(inputStream)) {
                message = container.getMesgBuilder().build();
            } else {
                message = null;
            }
            if (message == null) {
                throw new NoSuchElementException("");
            }
            for (Descriptors.FieldDescriptor descriptor : container.getSchema().getMessageDescriptor(colmeta.getValueClassName()).getFields()) {
                cachedValue.put(descriptor.getName(), message.getField(descriptor));
            }

        } catch (Exception ex) {
            errExist=true;
            log.error("{}", ex);
        }
        return !errExist?cachedValue:null;

    }
}
