package com.robin.gfdb.meta.model;

import com.robin.core.base.annotation.MappingEntity;
import com.robin.core.base.model.BaseObject;
import lombok.Data;

@MappingEntity("t_datafield_define")
@Data
public class DataFieldDefine extends BaseObject {
    private Long id;
    private Long collectionId;
    private Integer sort;
    private String columnType;
    private String columnName;
    private Integer length;
    private Integer precise;
    private Integer scale;
    private String dateFormat;
    private String nominalValues;
    private boolean required;


}
