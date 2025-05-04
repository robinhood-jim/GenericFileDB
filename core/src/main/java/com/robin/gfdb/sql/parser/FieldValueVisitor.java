package com.robin.gfdb.sql.parser;

import com.robin.gfdb.sql.calculate.Calculator;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.util.SqlVisitor;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;


public class FieldValueVisitor implements SqlVisitor<Object> {

    private SqlSegment segment;
    //private ThreadLocal<Stack<Object>> stackLocal=ThreadLocal.withInitial(Stack::new);
    private Calculator ca;
    public FieldValueVisitor(Calculator ca){
        this.ca=ca;
        this.segment=ca.getSegment();
    }
    public Object doCalculate(){
        ca.setColumnName(!ObjectUtils.isEmpty(ca.getValueParts().getAliasName()) ? ca.getValueParts().getAliasName() : ca.getValueParts().getIdentifyColumn());
        Object obj= ca.getValueParts().getNode().accept(this);
        ca.getOutputRecord().put(ca.getColumnName(),obj);
        return obj;
    }
    public Object doCalculate(SqlNode node){
        return node.accept(this);
    }


    @Override
    public Object visit(SqlLiteral sqlLiteral) {
        if(!SqlCharStringLiteral.class.isAssignableFrom(sqlLiteral.getClass())) {
            return sqlLiteral.getValue();
        }else{
            return sqlLiteral.toString();
        }
    }

    @Override
    public Object visit(SqlCall selected) {
        SqlNode processNode=selected;
        CommSqlParser.ValueParts parts=ca.getValueParts();
        if (SqlKind.AS.equals(selected.getKind())) {
            List<SqlNode> columnNodes = selected.getOperandList();
            parts.setAliasName(columnNodes.get(1).toString());
            processNode=columnNodes.get(0);
        }
        if (SqlKind.IDENTIFIER.equals(processNode.getKind())) {
            if(!ObjectUtils.isEmpty(ca.getInputRecord().get(((SqlIdentifier)processNode).getSimple()))){
                ca.setLeftValue(ca.getInputRecord().get(((SqlIdentifier)processNode).getSimple()));
            }
        } else if (SqlKind.CASE.equals(processNode.getKind())) {
            ca.setLeftValue(ca.getInputRecord().get(parts.getIdentifyColumn()));
            if (!ObjectUtils.isEmpty(ca.getLeftValue()) && parts.getCaseMap().containsKey(ca.getLeftValue().toString())) {
                ca.setLeftValue(parts.getCaseMap().get(ca.getLeftValue()).getNode().accept(this));
            }
        } else if (SqlKind.FUNCTION.contains(processNode.getKind())) {
            List<SqlNode> funcNodes = ((SqlBasicCall) processNode).getOperandList();
            String functionName=((SqlBasicCall) processNode).getOperator().toString();
            List<Object> expressions=new ArrayList<>();
            for(SqlNode node1:funcNodes){
                expressions.add(node1.accept(this));
            }
            switch (functionName.toLowerCase()){
                case "substr":
                    Assert.isTrue(expressions.size()>=3,"substr take more than three parameter");
                    String baseStr=expressions.get(0).toString();
                    Assert.isTrue(Integer.class.isAssignableFrom(expressions.get(1).getClass()) && Integer.class.isAssignableFrom(expressions.get(2).getClass()),"");
                    ca.setLeftValue(expressions.get(1));
                    ca.setRightValue(expressions.get(2));
                    if(baseStr.length()<(Integer)ca.getLeftValue()+(Integer)ca.getRightValue()){
                        ca.setRightValue(baseStr.length()-(Integer) ca.getLeftValue());
                    }
                    ca.setLeftValue(baseStr.substring((Integer) ca.getLeftValue(),(Integer)ca.getRightValue()));
                    break;
                case "trim":
                    if(!ObjectUtils.isEmpty(expressions.get(0))){
                        ca.setLeftValue(expressions.get(0).toString().trim());
                    }
                    break;
                case "concat":
                    if (ca.getBuilder().length() > 0) {
                        ca.getBuilder().delete(0, ca.getBuilder().length());
                    }
                    for(Object obj:expressions){
                        if(!ObjectUtils.isEmpty(obj)){
                            ca.getBuilder().append(obj);
                        }
                    }
                    ca.setLeftValue(ca.getBuilder().toString());
                    break;

            }
        }else if(SqlKind.IN.equals(processNode.getKind()) || SqlKind.NOT_IN.equals(processNode.getKind())){
            List<SqlNode> sqlNodes = ((SqlBasicCall) processNode).getOperandList();
            SqlIdentifier identifier = (SqlIdentifier) sqlNodes.get(0);
            Set<String> inSets = ca.getSegment().getInPartMap().get(segment.getNodeStringMap().get(identifier.hashCode()));
            ca.setLeftValue(false);
            if(!ObjectUtils.isEmpty(ca.getInputRecord().get(identifier.getSimple()))){
                ca.setLeftValue(SqlKind.IN.equals(processNode.getKind())?inSets.contains(ca.getInputRecord().get(identifier.getSimple())):!inSets.contains(ca.getInputRecord().get(identifier.getSimple())));
            }
        }
        else if (SqlBinaryOperator.class.isAssignableFrom(((SqlCall)processNode).getOperator().getClass())) {
            SqlKind operator=selected.getOperator().getKind();
            ca.setLeftValue(selected.getOperandList().get(0).accept(this));
            ca.setRightValue(selected.getOperandList().get(1).accept(this));
            if(Number.class.isAssignableFrom(ca.getLeftValue().getClass()) && Number.class.isAssignableFrom(ca.getRightValue().getClass())) {
                switch (operator) {
                    case PLUS:
                        ca.setLeftValue(((Number)ca.getLeftValue()).doubleValue()+((Number)ca.getRightValue()).doubleValue());
                        break;
                    case MINUS:
                        ca.setLeftValue(((Number)ca.getLeftValue()).doubleValue()-((Number)ca.getRightValue()).doubleValue());
                        break;
                    case TIMES:
                        ca.setLeftValue(((Number)ca.getLeftValue()).doubleValue()*((Number)ca.getRightValue()).doubleValue());
                        break;
                    case DIVIDE:
                        ca.setLeftValue(((Number)ca.getLeftValue()).doubleValue()/((Number)ca.getRightValue()).doubleValue());
                        break;
                    default:
                        throw new RuntimeException("Unsupported operator: " + operator);
                }
            }
        }
        return ca.getLeftValue();
    }

    @Override
    public Object visit(SqlNodeList sqlNodeList) {
        return null;
    }

    @Override
    public Object visit(SqlIdentifier sqlIdentifier) {
        if(!ObjectUtils.isEmpty(ca.getInputRecord().get(sqlIdentifier.getSimple()))){
            return ca.getInputRecord().get(sqlIdentifier.getSimple());
        }
        return null;
    }

    @Override
    public Object visit(SqlDataTypeSpec sqlDataTypeSpec) {
        return null;
    }

    @Override
    public Object visit(SqlDynamicParam sqlDynamicParam) {
        return null;
    }

    @Override
    public Object visit(SqlIntervalQualifier sqlIntervalQualifier) {
        return null;
    }
}
