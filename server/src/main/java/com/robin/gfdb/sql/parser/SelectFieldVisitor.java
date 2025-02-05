package com.robin.gfdb.sql.parser;

import com.google.common.collect.Sets;
import lombok.NonNull;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.util.SqlVisitor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.robin.gfdb.sql.parser.CommSqlParser.parseCase;
import static com.robin.gfdb.sql.parser.CommSqlParser.setAliasName;

public class SelectFieldVisitor implements SqlVisitor<CommSqlParser.ValueParts> {
    private List<CommSqlParser.ValueParts> valueParts = new ArrayList<>();
    private SqlSegment segment;
    private Map<Integer, Integer> newColumnPosMap;
    private String newColumnPrefix;

    public SelectFieldVisitor(@NonNull SqlSegment segment,String newColumnPrefix,Map<Integer,Integer> newColumnPosMap){
        this.segment=segment;
        this.newColumnPrefix=newColumnPrefix;
        this.newColumnPosMap=newColumnPosMap;
    }
    @Override
    public CommSqlParser.ValueParts visit(SqlLiteral sqlLiteral) {
        CommSqlParser.ValueParts parts=new CommSqlParser.ValueParts(sqlLiteral);
        parts.setConstantValue(sqlLiteral);
        valueParts.add(parts);
        return parts;
    }

    @Override
    public CommSqlParser.ValueParts visit(SqlCall selected) {
        CommSqlParser.ValueParts parts=new CommSqlParser.ValueParts(selected);
        SqlNode processNode=selected;
        if (SqlKind.AS.equals(selected.getKind())) {
            List<SqlNode> columnNodes = selected.getOperandList();
            parts.setAliasName(columnNodes.get(1).toString());
            processNode=columnNodes.get(0);
        }
        if (SqlKind.IDENTIFIER.equals(processNode.getKind())) {
            parts.setIdentifyColumn(processNode.toString());
        } else if (SqlKind.CASE.equals(processNode.getKind())) {
            parseCase((SqlCase) processNode, parts);
            setAliasName(newColumnPrefix, newColumnPosMap, parts);
        } else if (SqlKind.FUNCTION.contains(processNode.getKind())) {
            List<SqlNode> funcNodes = ((SqlBasicCall) processNode).getOperandList();
            parts.setFunctionName(((SqlBasicCall) processNode).getOperator().toString());
            parts.setFunctionParams(funcNodes);
            if(SqlBasicCall.class.isAssignableFrom(funcNodes.get(0).getClass())){
                parts.setNodeString(funcNodes.get(0).toString());
                parts.setCalculator(funcNodes.get(0));
                segment.setSelectHasFourOperations(true);
            }
            parts.setSqlKind(processNode.getKind());
            setAliasName(newColumnPrefix, newColumnPosMap, parts);
        }else if(SqlKind.IN.equals(processNode.getKind()) || SqlKind.NOT_IN.equals(processNode.getKind())){
            List<SqlNode> sqlNodes = ((SqlBasicCall) processNode).getOperandList();
            SqlIdentifier identifier = (SqlIdentifier) sqlNodes.get(0);
            Set<String> sets = Sets.newHashSet();
            for (int i = 1; i < sqlNodes.size(); i++) {
                sets.add(sqlNodes.get(i).toString());
            }
            segment.getInPartMap().put(identifier.toString(),sets);
        }
        else if (SqlBasicCall.class.isAssignableFrom(processNode.getClass())) {
            parts.setNodeString(processNode.toString());
            parts.setCalculator(processNode);
            segment.setSelectHasFourOperations(true);
            setAliasName(newColumnPrefix, newColumnPosMap, parts);
        }
        valueParts.add(parts);
        return parts;
    }


    @Override
    public CommSqlParser.ValueParts visit(SqlNodeList sqlNodeList) {
        return null;
    }

    @Override
    public CommSqlParser.ValueParts visit(SqlIdentifier sqlIdentifier) {
        CommSqlParser.ValueParts parts=new CommSqlParser.ValueParts(sqlIdentifier);
        parts.setIdentifyColumn(sqlIdentifier.getSimple());
        valueParts.add(parts);
        return parts;
    }

    @Override
    public CommSqlParser.ValueParts visit(SqlDataTypeSpec sqlDataTypeSpec) {
        return null;
    }

    @Override
    public CommSqlParser.ValueParts visit(SqlDynamicParam sqlDynamicParam) {
        return null;
    }

    @Override
    public CommSqlParser.ValueParts visit(SqlIntervalQualifier sqlIntervalQualifier) {
        return null;
    }

    public List<CommSqlParser.ValueParts> getValueParts() {
        return valueParts;
    }
}
