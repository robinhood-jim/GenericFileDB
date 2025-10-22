package com.robin.gfdb.storage;

import com.robin.core.base.exception.OperationNotSupportException;
import com.robin.core.base.util.Const;
import com.robin.core.fileaccess.meta.DataCollectionMeta;
import com.robin.gfdb.cloud.*;

public class FileSystemFactory {
    public static AbstractFileSystem getFileSystemByIdentifier(DataCollectionMeta colmeta){
        Const.FILESYSTEM filesystem= Const.FILESYSTEM.forName(colmeta.getFsType());
        AbstractFileSystem accessor=null;
        switch (filesystem){
            case LOCAL:
                accessor=LocalFileSystem.getInstance();
                break;
            case VFS:
            case FTP:
            case SFTP:
                accessor=new ApacheVfsFileSystem();
                accessor.init(colmeta);
                break;
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
                throw new OperationNotSupportException("unsupport fsType "+colmeta.getFsType());
        }
        return accessor;
    }
}
