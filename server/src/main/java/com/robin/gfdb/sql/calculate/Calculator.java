package com.robin.gfdb.sql.calculate;


import com.robin.gfdb.sql.parser.CommSqlParser;
import com.robin.gfdb.sql.parser.SqlSegment;
import lombok.Getter;
import lombok.Setter;
import org.apache.calcite.sql.SqlNode;
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
    private StringBuilder builder;
    private Map<String,String> stringLiteralMap=new WeakHashMap<>();
    protected boolean busyTag=false;
    private Slot slot;



    public Calculator(){

    }
    public Calculator(Slot slot){
        this.slot=slot;
    }
    public boolean doCompare(SqlNode node){
        SqlFunctions.doCompare(segment,this,node);
        return runValue;
    }
    public boolean doCalculate(CommSqlParser.ValueParts valueParts){
        return SqlFunctions.doCalculate(this,valueParts);
    }
    public boolean walkTree(SqlNode node){
        return SqlFunctions.walkTree(segment,this,node);
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
        inputRecord.clear();
        outputRecord.clear();
        inputRecord=null;
        outputRecord=null;
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
}
