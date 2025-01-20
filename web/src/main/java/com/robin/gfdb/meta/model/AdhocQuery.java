package com.robin.gfdb.meta.model;

import com.robin.core.base.annotation.MappingEntity;
import com.robin.core.base.annotation.MappingField;
import com.robin.core.base.model.BaseObject;

import java.time.LocalDateTime;

@MappingEntity("t_adhoc_query")
public class AdhocQuery extends BaseObject {
    @MappingField(primary = true,increment = true)
    private Long id;
    private Long userId;
    private LocalDateTime queryTs;
    private LocalDateTime finishTs;
    private String tableName;
    private String executeSql;
    private String outputPath;
    private Long pageSize;

}
