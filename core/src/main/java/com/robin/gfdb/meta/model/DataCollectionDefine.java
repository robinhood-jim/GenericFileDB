package com.robin.gfdb.meta.model;

import com.robin.core.base.annotation.MappingEntity;
import com.robin.core.base.annotation.MappingField;
import com.robin.core.base.model.BaseObject;
import lombok.Data;

@MappingEntity("t_datacollection_define")
@Data
public class DataCollectionDefine extends BaseObject {
    @MappingField(primary = true,increment = true)
    private Long id;
    private String name;
    private String resPath;
    private String fileFormat;
    private Long storageId;
    private String fields;
    private Long owner;


}
