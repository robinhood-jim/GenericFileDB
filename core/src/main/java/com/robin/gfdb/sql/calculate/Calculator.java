package com.robin.gfdb.sql.calculate;


import com.robin.gfdb.sql.parser.CommSqlParser;
import com.robin.gfdb.sql.parser.FieldValueVisitor;
import com.robin.gfdb.sql.parser.SqlSegment;
import lombok.Getter;
import lombok.Setter;
import org.apache.calcite.sql.SqlNode;
import org.springframework.util.CollectionUtils;
import stormpot.Poolable;
import stormpot.Slot;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.WeakHashMap;

@Setter
@Getter
public class Calculator implements Closeable, Poolable {
    private Object leftValue;
    private Object rightValue;
    private Boolean runValue;
    private String cmpColumn;
    private String columnName;
    private SqlSegment segment;
    private CommSqlParser.ValueParts valueParts;
    private Map<String,Object> inputRecord;
    private Map<String,Object> outputRecord;
    private StringBuilder builder=new StringBuilder();
    private Map<String,String> stringLiteralMap=new WeakHashMap<>();
    protected boolean busyTag=false;
    private Slot slot;
    private FieldValueVisitor visitor;

    public Calculator(Slot slot){
        this.slot=slot;
        visitor=new FieldValueVisitor(this);
    }
    public boolean doCompare(SqlNode node,CalculatorPool pool){
        SqlFunctions.doCompare(segment,this,node,pool);
        return runValue;
    }
    public boolean doCalculate(){
        return visitor.doCalculate()!=null;
        //return SqlFunctions.doCalculate(this,valueParts);
    }
    public boolean walkTree(SqlNode node,CalculatorPool pool){
        return SqlFunctions.walkTree(segment,this,node,pool);
    }
    public void clear(){
        leftValue=null;
        rightValue=null;
        runValue=null;
        columnName=null;
    }



    @Override
    public void close() throws IOException {
        leftValue=null;
        rightValue=null;
        runValue=null;
        columnName=null;
        if(!CollectionUtils.isEmpty(inputRecord)) {
            inputRecord.clear();
            inputRecord=null;
        }
        if(!CollectionUtils.isEmpty(outputRecord)) {
            outputRecord.clear();
            outputRecord=null;
        }
        stringLiteralMap.clear();
        segment=null;
        valueParts=null;
        setBusyTag(false);
    }
    public void finish(){
        setBusyTag(false);
    }


    public void setBusyTag(boolean tag){
        this.busyTag=tag;
    }

    public boolean isBusyTag() {
        return busyTag;
    }

    @Override
    public void release() {
        setBusyTag(false);
        slot.release(this);
    }
    public void CopyTo(Calculator other){
        other.setValueParts(valueParts);
        other.setCmpColumn(cmpColumn);
        other.setInputRecord(inputRecord);
        other.setOutputRecord(outputRecord);
        other.setStringLiteralMap(stringLiteralMap);
        if(this.builder.length()>0){
            other.builder.append(this.builder);
        }
    }
}
