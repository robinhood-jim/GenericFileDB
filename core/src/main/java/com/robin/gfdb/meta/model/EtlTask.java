package com.robin.gfdb.meta.model;

import com.robin.core.base.annotation.MappingEntity;
import com.robin.core.base.annotation.MappingField;
import com.robin.core.base.model.BaseObject;

@MappingEntity("t_etl_task")
public class EtlTask extends BaseObject {
    @MappingField(primary = true,increment = true)
    private Long id;
    private Long jobId;
    private String runningCycle;
    private String outputPath;
    private Integer status;


}
