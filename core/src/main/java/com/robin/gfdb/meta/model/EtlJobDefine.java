package com.robin.gfdb.meta.model;

import com.robin.core.base.annotation.MappingEntity;
import com.robin.core.base.model.BaseObject;

import java.time.LocalDateTime;

@MappingEntity("t_etljob_define")
public class EtlJobDefine extends BaseObject {
    private Long id;
    private String defineYaml;
    private Long userId;
    private String cycleType;
    private String cronTrigger;
    private LocalDateTime startTs;
    private LocalDateTime endTs;
    private Integer status;

}
