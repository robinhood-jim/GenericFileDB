package com.robin.gfdb.record.reader;

import com.robin.core.base.util.Const;
import com.robin.core.base.util.ResourceConst;
import com.robin.core.fileaccess.meta.DataCollectionMeta;
import com.robin.core.fileaccess.util.ResourceUtil;
import com.robin.gfdb.hdfs.HDFSUtil;
import com.robin.gfdb.record.utils.MockFileSystem;
import com.robin.gfdb.record.utils.OrcUtil;
import com.robin.gfdb.storage.AbstractFileSystem;
import com.robin.gfdb.utils.SysUtils;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.List;
import java.util.Map;

public class OrcFileReader extends AbstractFileReader{
    private Double allowOffHeapDumpLimit= ResourceConst.ALLOWOUFHEAPMEMLIMIT;
    int maxRow=0;
    int currentRow=0;
    private TypeDescription schema;
    private RecordReader rows ;
    private VectorizedRowBatch batch ;
    private MemorySegment segment;
    private List<TypeDescription> fields;
    private Configuration conf;
    private FileSystem fs;
    private File tmpFile;
    private Reader oreader;
    private Reader.Options options;

    public OrcFileReader(DataCollectionMeta colmeta, AbstractFileSystem fileSystem) {
        super(colmeta, fileSystem);
        setIdentifier(Const.FILEFORMATSTR.ORC.getValue());
        useRawInputStream=true;
    }


    @Override
    public void init() throws IOException {
        super.init();
        String processPath=colmeta.getPath();
        try {
            if (!CollectionUtils.isEmpty(colmeta.getResourceCfgMap()) && colmeta.getResourceCfgMap().containsKey(ResourceConst.ALLOWOFFHEAPKEY)) {
                allowOffHeapDumpLimit = Double.parseDouble(colmeta.getResourceCfgMap().get(ResourceConst.ALLOWOFFHEAPKEY).toString());
            }
            if (Const.FILESYSTEM.HDFS.getValue().equals(colmeta.getFsType())) {
                HDFSUtil util = new HDFSUtil(colmeta);
                conf = util.getConfig();
                fs = FileSystem.get(conf);
            } else {
                conf = new Configuration(false);
                if (Const.FILESYSTEM.LOCAL.getValue().equals(colmeta.getFsType())) {
                    fs = FileSystem.get(new Configuration());
                    if (!processPath.startsWith("file:/")) {
                        processPath = new File(processPath).toURI().toString();
                    }
                } else {
                    inputStream = fileSystem.getRawInputStream(ResourceUtil.getProcessPath(colmeta.getPath()));
                    long size = fileSystem.getInputStreamSize(ResourceUtil.getProcessPath(colmeta.getPath()));
                    Double freeMemory = SysUtils.getFreeMemory();
                    if (size < ResourceConst.MAX_ARRAY_SIZE && freeMemory > allowOffHeapDumpLimit) {
                        segment = MemorySegmentFactory.allocateOffHeapUnsafeMemory((int) size, this, new Thread() {
                        });
                        ByteBuffer byteBuffer = segment.getOffHeapBuffer();
                        try (ReadableByteChannel channel = Channels.newChannel(inputStream)) {
                            org.apache.commons.io.IOUtils.readFully(channel, byteBuffer);
                            byteBuffer.position(0);
                        }
                        fs = new MockFileSystem(conf, byteBuffer);
                    } else {
                        String tmpPath = com.robin.core.base.util.FileUtils.getWorkingPath(colmeta);
                        String tmpFilePath = "file:///" + tmpPath + ResourceUtil.getProcessFileName(colmeta.getPath());
                        tmpFile = new File(new URL(tmpFilePath).toURI());
                        copyToLocal(tmpFile, inputStream);
                        fs = FileSystem.get(new Configuration());
                        processPath = tmpFilePath;
                    }
                }
            }
            schema= OrcUtil.getSchema(colmeta);
            oreader = OrcFile.createReader(new Path(processPath),OrcFile.readerOptions(conf).filesystem(fs));
            if(schema==null) {
                schema = oreader.getSchema();
            }
            if(!ObjectUtils.isEmpty(super.segment) && !ObjectUtils.isEmpty(super.segment.getWhereCause()) &&!super.segment.isConditionHasFunction() && !super.segment.isConditionHasFunction() && !super.segment.isHasRightColumnCmp()){
                SearchArgument.Builder argumentBuilder= SearchArgumentFactory.newBuilder();
                OrcUtil.walkCondition(this,super.segment.getWhereCause(),argumentBuilder);
                options= new Reader.Options().schema(schema).allowSARGToFilter(true)
                        .searchArgument(argumentBuilder.build(),getColumnNames().toArray(new String[0]));
            }
            oreader =OrcFile.createReader(new Path(processPath),OrcFile.readerOptions(conf).filesystem(fs));
            if(options!=null){
                rows=oreader.rows(options);
            }else {
                rows = oreader.rows();
            }
            fields=schema.getChildren();
            batch= schema.createRowBatch();
            rows.nextBatch(batch);
            maxRow=batch.size;

        }catch (Exception ex){

        }
    }

    @Override
    public Map<String, Object> pullNext() {
        try{
            cachedValue.clear();
            if (maxRow > 0 && currentRow >= maxRow ) {
                currentRow = 0;
                boolean exists=rows.nextBatch(batch);
                if(!exists){
                    return null;
                }
                maxRow = batch.size;
            }
            List<String> fieldNames=schema.getFieldNames();
            if(!CollectionUtils.isEmpty(fields)){
                for(int i=0;i<fields.size();i++){
                    OrcUtil.wrapValue(fields.get(i),fieldNames.get(i),batch.cols[i],currentRow,cachedValue);
                }
            }
            currentRow++;
        }catch (Exception ex){

        }

        return null;
    }


}
