package com.robin.gfdb.meta.model;

import com.robin.core.base.annotation.MappingEntity;
import com.robin.core.base.annotation.MappingField;
import com.robin.core.base.model.BaseObject;
import lombok.Data;

import java.time.LocalDateTime;

@MappingEntity("t_adhoc_query")
@Data
public class AdhocQuery extends BaseObject {
    @MappingField(primary = true,increment = true)
    private Long id;
    private String queryId;
    private Long userId;
    private Long dataCollectionId;
    private LocalDateTime queryTs;
    private LocalDateTime finishTs;
    private String tableName;
    private String executeSql;
    private String outputPath;
    private Long pageSize;

}
