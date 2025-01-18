package com.robin.gfdb.cloud;

import com.robin.core.base.exception.OperationNotSupportException;
import com.robin.core.base.util.Const;
import com.robin.core.fileaccess.meta.DataCollectionMeta;
import com.robin.gfdb.storage.AbstractFileSystem;

public class CloudStorageFileSystemFactory {
    public static AbstractFileSystem getAccessorByIdentifier(DataCollectionMeta colmeta,String identifier){
        Const.FILESYSTEM filesystem= Const.FILESYSTEM.valueOf(identifier);
        AbstractFileSystem accessor=null;
        switch (filesystem){
            case BAIDU_BOS:
                accessor= BOSFileSystem.Builder.builder().withMetaConfig(colmeta).build();
                break;
            case TENCENT:
                accessor= COSFileSystem.Builder.builder().withMetaConfig(colmeta).build();
                break;
            case S3:
                accessor= S3FileSystem.Builder.builder().withMetaConfig(colmeta).build();
                break;
            case HUAWEI_OBS:
                accessor= BOSFileSystem.Builder.builder().withMetaConfig(colmeta).build();
                break;
            case ALIYUN:
                accessor= OSSFileSystem.Builder.builder().withMetaConfig(colmeta).build();
                break;
            case QINIU:
                accessor= QiniuFileSystem.Builder.builder().withMetaConfig(colmeta).build();
                break;
            case MINIO:
                accessor= MinioFileSystem.Builder.builder().withMetaConfig(colmeta).build();
                break;
            default:
                throw new OperationNotSupportException("unsupport fsType "+identifier);
        }
        return accessor;
    }

}
