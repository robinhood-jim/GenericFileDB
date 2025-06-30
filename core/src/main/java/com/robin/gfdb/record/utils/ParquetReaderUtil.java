package com.robin.gfdb.record.utils;

import com.google.common.collect.Sets;
import com.robin.core.base.exception.OperationNotSupportException;
import com.robin.core.base.util.Const;
import com.robin.core.fileaccess.meta.DataSetColumnMeta;
import com.robin.gfdb.record.reader.AbstractFileReader;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;

import java.util.List;
import java.util.stream.Collectors;

import static org.apache.parquet.filter2.predicate.FilterApi.*;


public class ParquetReaderUtil {
    private ParquetReaderUtil(){

    }
    public static String parseColumnType(PrimitiveType type){
        String rettype= Const.META_TYPE_STRING;
        if(type.getPrimitiveTypeName().equals(PrimitiveType.PrimitiveTypeName.INT32)){
            rettype=Const.META_TYPE_INTEGER;
        }else if(type.getPrimitiveTypeName().equals(PrimitiveType.PrimitiveTypeName.INT64)){
            if(LogicalTypeAnnotation.dateType().equals(type.getLogicalTypeAnnotation()) || LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS).equals(type.getLogicalTypeAnnotation())){
                rettype=Const.META_TYPE_TIMESTAMP;
            }else {
                rettype = Const.META_TYPE_BIGINT;
            }
        }else if(type.getPrimitiveTypeName().equals(PrimitiveType.PrimitiveTypeName.DOUBLE)){
            rettype=Const.META_TYPE_DOUBLE;
        }else if(type.getPrimitiveTypeName().equals(PrimitiveType.PrimitiveTypeName.FLOAT)){
            rettype=Const.META_TYPE_DOUBLE;
        }else if(type.getPrimitiveTypeName().equals(PrimitiveType.PrimitiveTypeName.INT96)){
            rettype=Const.META_TYPE_TIMESTAMP;
        }
        return rettype;
    }
    public static FilterPredicate parseOperator(AbstractFileReader reader,SqlNode node){
        List<SqlNode> nodes=((SqlBasicCall)node).getOperandList();
        Object cmpValue=((SqlLiteral)nodes.get(1)).getValue();
        return parseOperator(reader,nodes.get(0).toString(),node.getKind(),cmpValue);
    }
    private static FilterPredicate parseOperator(AbstractFileReader reader,String columnName, SqlKind operator, Object value) {
        FilterPredicate predicate;
        DataSetColumnMeta meta = reader.getColumnMap().get(columnName);

        if (meta == null) {
            meta = reader.getColumnMap().get(columnName.toUpperCase());
        }
        switch (operator) {
            case GREATER_THAN:
                if (Const.META_TYPE_INTEGER.equals(meta.getColumnType())) {
                    predicate = gt(intColumn(columnName), Integer.parseInt(value.toString()));
                } else if (Const.META_TYPE_DOUBLE.equals(meta.getColumnType()) || Const.META_TYPE_DECIMAL.equals(meta.getColumnType())) {
                    predicate = gt(doubleColumn(columnName), Double.parseDouble(value.toString()));
                } else if (Const.META_TYPE_BIGINT.equals(meta.getColumnType()) || Const.META_TYPE_TIMESTAMP.equals(meta.getColumnType())) {
                    predicate = gt(longColumn(columnName), Long.parseLong(value.toString()));
                } else {
                    throw new OperationNotSupportException("type not support");
                }
                break;
            case GREATER_THAN_OR_EQUAL:
                if (Const.META_TYPE_INTEGER.equals(meta.getColumnType())) {
                    predicate = gtEq(intColumn(columnName), Integer.parseInt(value.toString()));
                } else if (Const.META_TYPE_DOUBLE.equals(meta.getColumnType()) || Const.META_TYPE_DECIMAL.equals(meta.getColumnType())) {
                    predicate = gtEq(doubleColumn(columnName), Double.parseDouble(value.toString()));
                } else if (Const.META_TYPE_BIGINT.equals(meta.getColumnType()) || Const.META_TYPE_TIMESTAMP.equals(meta.getColumnType())) {
                    predicate = gtEq(longColumn(columnName), Long.parseLong(value.toString()));
                } else {
                    throw new OperationNotSupportException("type not support");
                }
                break;
            case EQUALS:
                if (Const.META_TYPE_INTEGER.equals(meta.getColumnType())) {
                    predicate = eq(intColumn(columnName), Integer.parseInt(value.toString()));
                } else if (Const.META_TYPE_DOUBLE.equals(meta.getColumnType()) || Const.META_TYPE_DECIMAL.equals(meta.getColumnType())) {
                    predicate = eq(doubleColumn(columnName), Double.parseDouble(value.toString()));
                } else if (Const.META_TYPE_BIGINT.equals(meta.getColumnType()) || Const.META_TYPE_TIMESTAMP.equals(meta.getColumnType())) {
                    predicate = eq(longColumn(columnName), Long.parseLong(value.toString()));
                } else if (Const.META_TYPE_STRING.equals(meta.getColumnType())) {
                    predicate = eq(binaryColumn(columnName), Binary.fromString(value.toString()));
                } else {
                    throw new OperationNotSupportException("type not support");
                }
                break;
            case LESS_THAN:
                if (Const.META_TYPE_INTEGER.equals(meta.getColumnType())) {
                    predicate = lt(intColumn(columnName), Integer.parseInt(value.toString()));
                } else if (Const.META_TYPE_DOUBLE.equals(meta.getColumnType()) || Const.META_TYPE_DECIMAL.equals(meta.getColumnType())) {
                    predicate = lt(doubleColumn(columnName), Double.parseDouble(value.toString()));
                } else if (Const.META_TYPE_BIGINT.equals(meta.getColumnType()) || Const.META_TYPE_TIMESTAMP.equals(meta.getColumnType())) {
                    predicate = lt(longColumn(columnName), Long.parseLong(value.toString()));
                } else {
                    throw new OperationNotSupportException("type not support");
                }
                break;
            case LESS_THAN_OR_EQUAL:
                if (Const.META_TYPE_INTEGER.equals(meta.getColumnType())) {
                    predicate = ltEq(intColumn(columnName), Integer.parseInt(value.toString()));
                } else if (Const.META_TYPE_DOUBLE.equals(meta.getColumnType()) || Const.META_TYPE_DECIMAL.equals(meta.getColumnType())) {
                    predicate = ltEq(doubleColumn(columnName), Double.parseDouble(value.toString()));
                } else if (Const.META_TYPE_BIGINT.equals(meta.getColumnType()) || Const.META_TYPE_TIMESTAMP.equals(meta.getColumnType())) {
                    predicate = ltEq(longColumn(columnName), Long.parseLong(value.toString()));
                } else {
                    throw new OperationNotSupportException("type not support");
                }
                break;
            case NOT_EQUALS:
                if (Const.META_TYPE_INTEGER.equals(meta.getColumnType())) {
                    predicate = notEq(intColumn(columnName), Integer.parseInt(value.toString()));
                } else if (Const.META_TYPE_DOUBLE.equals(meta.getColumnType()) || Const.META_TYPE_DECIMAL.equals(meta.getColumnType())) {
                    predicate = notEq(doubleColumn(columnName), Double.parseDouble(value.toString()));
                } else if (Const.META_TYPE_BIGINT.equals(meta.getColumnType()) || Const.META_TYPE_TIMESTAMP.equals(meta.getColumnType())) {
                    predicate = notEq(longColumn(columnName), Long.parseLong(value.toString()));
                } else {
                    throw new OperationNotSupportException("type not support");
                }
                break;
            case IN:
                if (Const.META_TYPE_INTEGER.equals(meta.getColumnType())) {
                    predicate = in(intColumn(columnName), Sets.newHashSet(reader.getSegment().getInPartMap().get(columnName).stream().map(Integer::valueOf).collect(Collectors.toList())));
                } else if (Const.META_TYPE_DOUBLE.equals(meta.getColumnType()) || Const.META_TYPE_DECIMAL.equals(meta.getColumnType())) {
                    predicate = in(doubleColumn(columnName), Sets.newHashSet(reader.getSegment().getInPartMap().get(columnName).stream().map(Double::valueOf).collect(Collectors.toList())));
                } else if (Const.META_TYPE_BIGINT.equals(meta.getColumnType()) || Const.META_TYPE_TIMESTAMP.equals(meta.getColumnType())) {
                    predicate = in(longColumn(columnName), Sets.newHashSet(reader.getSegment().getInPartMap().get(columnName).stream().map(Long::valueOf).collect(Collectors.toList())));
                } else if (Const.META_TYPE_STRING.equals(meta.getColumnType())) {
                    predicate = in(binaryColumn(columnName), Sets.newHashSet(reader.getSegment().getInPartMap().get(columnName).stream().map(Binary::fromString).collect(Collectors.toList())));
                } else {
                    throw new OperationNotSupportException("type not support");
                }
                break;
            case NOT_IN:
                if (Const.META_TYPE_INTEGER.equals(meta.getColumnType())) {
                    predicate = notIn(intColumn(columnName), Sets.newHashSet(reader.getSegment().getInPartMap().get(columnName).stream().map(Integer::valueOf).collect(Collectors.toList())));
                } else if (Const.META_TYPE_DOUBLE.equals(meta.getColumnType()) || Const.META_TYPE_DECIMAL.equals(meta.getColumnType())) {
                    predicate = notIn(doubleColumn(columnName), Sets.newHashSet(reader.getSegment().getInPartMap().get(columnName).stream().map(Double::valueOf).collect(Collectors.toList())));
                } else if (Const.META_TYPE_BIGINT.equals(meta.getColumnType()) || Const.META_TYPE_TIMESTAMP.equals(meta.getColumnType())) {
                    predicate = notIn(longColumn(columnName), Sets.newHashSet(reader.getSegment().getInPartMap().get(columnName).stream().map(Long::valueOf).collect(Collectors.toList())));
                } else if (Const.META_TYPE_STRING.equals(meta.getColumnType())) {
                    predicate = notIn(binaryColumn(columnName), Sets.newHashSet(reader.getSegment().getInPartMap().get(columnName).stream().map(Binary::fromString).collect(Collectors.toList())));
                } else {
                    throw new OperationNotSupportException("type not support");
                }
                break;
            case LIKE:
                if (Const.META_TYPE_STRING.equals(meta.getColumnType())){
                    predicate= FilterApi.userDefined(FilterApi.binaryColumn(columnName),new CharLikePredicate(value.toString()));
                }else {
                    throw new OperationNotSupportException("type not support");
                }
                break;

            default:
                throw new OperationNotSupportException(" not supported!");

        }
        return predicate;
    }
}
