package com.robin.gfdb.record.reader;

import com.robin.core.base.exception.MissingConfigException;
import com.robin.core.base.util.Const;
import com.robin.core.base.util.ResourceConst;
import com.robin.core.fileaccess.meta.DataCollectionMeta;
import com.robin.core.fileaccess.util.ResourceUtil;
import com.robin.gfdb.hdfs.HDFSUtil;
import com.robin.gfdb.storage.AbstractFileSystem;
import com.robin.gfdb.utils.SysUtils;
import com.robin.gfdb.utils.avro.AvroUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.file.SeekableFileInput;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.hadoop.fs.AvroFSInput;
import org.apache.hadoop.fs.FSDataInputStream;
import org.springframework.util.ObjectUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.List;
import java.util.Map;

@Slf4j
public class AvroFileReader extends AbstractFileReader implements IDataFileReader{
    private Schema schema;
    private FileReader<GenericRecord> fileReader;
    private MemorySegment segment;
    private SeekableInput input = null;
    private Double allowOffHeapDumpLimit = ResourceConst.ALLOWOUFHEAPMEMLIMIT;
    private File tmpFile;

    public AvroFileReader(DataCollectionMeta colmeta, AbstractFileSystem fileSystem) {
        super(colmeta, fileSystem);
        setIdentifier(Const.FILEFORMATSTR.AVRO.getValue());
        useRawInputStream=true;
    }

    @Override
    public void init() throws IOException {
        super.init();
        schema = AvroUtils.getSchemaFromMeta(colmeta);
        if (colmeta.getSourceType().equals(ResourceConst.IngestType.TYPE_HDFS.getValue())) {
            HDFSUtil util = new HDFSUtil(colmeta);
            input = new AvroFSInput(new FSDataInputStream(inputStream), util.getHDFSFileSize(ResourceUtil.getProcessPath(colmeta.getPath())));
        }else{
            if (!ResourceConst.IngestType.TYPE_LOCAL.getValue().equals(colmeta.getSourceType())) {
                long size=fileSystem.getInputStreamSize(colmeta.getPath());
                Double freeMemory = SysUtils.getFreeMemory();
                if (size >=ResourceConst.MAX_ARRAY_SIZE || freeMemory < allowOffHeapDumpLimit) {
                    String tmpPath = FileUtils.getTempDirectoryPath() + ResourceUtil.getProcessFileName(colmeta.getPath());
                    tmpFile = new File(tmpPath);
                    copyToLocal(tmpFile, inputStream);
                    input = new SeekableFileInput(tmpFile);
                } else {
                    segment = MemorySegmentFactory.allocateOffHeapUnsafeMemory((int)size, this, new Thread() {});
                    ByteBuffer byteBuffer = segment.getOffHeapBuffer();
                    try (ReadableByteChannel channel = Channels.newChannel(inputStream)) {
                        IOUtils.readFully(channel, byteBuffer);
                        byteBuffer.position(0);
                    }
                    input = new SeekableByteBufferInputStream(segment.getOffHeapBuffer());
                }
            }else{
                input=new SeekableFileInput(new File(colmeta.getPath()));
            }
        }
        if(schema!=null) {
            GenericDatumReader<GenericRecord> dreader = new GenericDatumReader<>(schema);
            fileReader = new DataFileReader<>(input, dreader);
        }else {
            GenericDatumReader<GenericRecord> dreader = new GenericDatumReader<>();
            fileReader=new DataFileReader<>(input,dreader);
            schema=fileReader.getSchema();
        }
    }

    @Override
    public Map<String, Object> pullNext() {
        try{
            cachedValue.clear();
            if(fileReader.hasNext()){
                GenericRecord records = fileReader.next();
                List<Schema.Field> flist = schema.getFields();
                for (Schema.Field f : flist) {
                    if (!ObjectUtils.isEmpty(records.get(f.name()))) {
                        cachedValue.put(f.name(), records.get(f.name()).toString());
                    }
                }
            }
        }catch (Exception ex){
            throw new MissingConfigException(ex);
        }
        return cachedValue;
    }

    @Override
    public void close() throws IOException {
        if(!ObjectUtils.isEmpty(fileReader)) {
            fileReader.close();
        }
        if(!ObjectUtils.isEmpty(input)){
            input.close();
        }
        if(!ObjectUtils.isEmpty(tmpFile)){
            FileUtils.deleteQuietly(tmpFile);
        }
        if(!ObjectUtils.isEmpty(segment)){
            segment.free();
        }
        super.close();
    }
    static class SeekableByteBufferInputStream extends InputStream implements SeekableInput {
        private final byte[] oneByte = new byte[1];

        private ByteBuffer byteBuffer;

        SeekableByteBufferInputStream(ByteBuffer byteBuffer) throws IOException {
            this.byteBuffer = byteBuffer;
        }

        public void seek(long p) throws IOException {
            if (p < 0L) {
                throw new IOException("Illegal seek: " + p);
            } else {
                byteBuffer.position((int) p);
            }
        }

        public long tell() throws IOException {
            return byteBuffer.position();
        }

        public long length() throws IOException {
            return byteBuffer.capacity();
        }

        public int read(byte[] b) throws IOException {
            return read(b, 0, b.length);
        }

        public int read(byte[] b, int off, int len) throws IOException {
            if (byteBuffer.remaining() == 0) {
                return -1;
            }
            if (len > byteBuffer.remaining()) {
                len = byteBuffer.remaining();
            }
            byteBuffer.get(b, off, len);
            return len;
        }

        public int read() throws IOException {
            int n = this.read(this.oneByte, 0, 1);
            return n == 1 ? this.oneByte[0] & 255 : n;
        }

        public long skip(long skip) throws IOException {
            long newPos = byteBuffer.position() + skip;
            if (newPos > byteBuffer.remaining()) {
                skip = byteBuffer.remaining();
            }
            byteBuffer.position(byteBuffer.position() + (int) skip);
            return skip;
        }

        public void close() throws IOException {
            super.close();
        }

        public int available() throws IOException {
            long remaining = length() - tell();
            return remaining > 2147483647L ? Integer.MAX_VALUE : (int) remaining;
        }
    }
}
