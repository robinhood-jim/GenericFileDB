package com.robin.gfdb.record.utils;


import com.robin.core.fileaccess.meta.DataCollectionMeta;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.InputFile;

import java.io.IOException;
import java.util.Map;

public class CustomParquetReader<T> extends ParquetReader<T> {
    private DataCollectionMeta colMeta;
    public CustomParquetReader(Configuration conf, Path file, ReadSupport readSupport,DataCollectionMeta colMeta) throws IOException {
        super(conf,file, readSupport);
        this.colMeta=colMeta;
    }

    public static  Builder builder(InputFile file,DataCollectionMeta colMeta) {
        return new Builder(file,colMeta);
    }
    public static class Builder extends ParquetReader.Builder<Map<String,Object>> {
        private boolean enableCompatibility;
        private DataCollectionMeta colMeta;
        /** @deprecated */
        @Deprecated
        private Builder(Path path,DataCollectionMeta colMeta) {
            super(path);
            this.colMeta=colMeta;
        }

        private Builder(InputFile file,DataCollectionMeta colMeta) {
            super(file);
            this.colMeta=colMeta;
        }



        public Builder disableCompatibility() {
            return this;
        }

        public Builder withCompatibility(boolean enableCompatibility) {
            this.enableCompatibility = enableCompatibility;
            return this;
        }

        @Override
        protected ReadSupport<Map<String,Object>> getReadSupport() {
            return new CustomRowReadSupport(colMeta);
        }

    }
}
