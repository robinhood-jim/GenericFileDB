package com.robin.gfdb.meta.model;

import com.robin.core.base.annotation.MappingEntity;
import com.robin.core.base.annotation.MappingField;
import com.robin.core.base.model.BaseObject;
import lombok.Data;


@MappingEntity("t_storage_define")
@Data
public class FileStorageDefine extends BaseObject {
    @MappingField(primary = true,increment = true)
    private Long id;
    private String storageType;
    private String configParam;
    private String creator;

}
