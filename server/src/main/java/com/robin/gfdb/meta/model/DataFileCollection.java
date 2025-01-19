package com.robin.gfdb.meta.model;

import com.robin.core.base.annotation.MappingEntity;
import com.robin.core.base.model.BaseObject;

@MappingEntity("t_datafile_collection")
public class DataFileCollection extends BaseObject {
    private Long id;
    private String resPath;
    private Long storageId;
    private String fields;


}
