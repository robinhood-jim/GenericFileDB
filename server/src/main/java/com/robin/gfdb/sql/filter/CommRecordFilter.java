package com.robin.gfdb.sql.filter;

import com.google.common.util.concurrent.*;
import com.robin.core.base.exception.GenericException;
import com.robin.gfdb.sql.calculate.Calculator;
import com.robin.gfdb.sql.calculate.CalculatorCallable;
import com.robin.gfdb.sql.calculate.CalculatorPool;
import com.robin.gfdb.sql.parser.CommSqlParser;
import com.robin.gfdb.sql.parser.SqlSegment;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlNode;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
public class CommRecordFilter {
    private static CalculatorPool caPool;
    private static ListeningExecutorService pool;
    static {
        caPool=new CalculatorPool();
        pool= MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(20));
    }

    public static boolean doesRecordAcceptable(SqlSegment segment, Map<String, Object> inputRecord) {
        SqlNode whereNode = segment.getWhereCause();
        if (CollectionUtils.isEmpty(segment.getWherePartsMap())) {
            segment.setWherePartsMap(segment.getWhereColumns().stream().collect(Collectors.toMap(CommSqlParser.ValueParts::getNodeString, Function.identity())));
        }
        Calculator calculator=null;
        try{
            calculator=caPool.borrowObject();
            calculator.setSegment(segment);
            calculator.setInputRecord(inputRecord);
            return calculator.walkTree(whereNode);
        }catch (Exception ex){
            log.error("{}",ex.getMessage());
        }finally {
            if(calculator!=null) {
                caPool.returnObject(calculator);
            }
        }
        return false;
    }
    /**
     * do Calculate async using ListenableFuture
     * @param segment
     * @param inputRecord
     * @param newRecord
     * @throws Exception
     */
    public static void doAsyncCalculator(SqlSegment segment,Map<String,Object> inputRecord,Map<String,Object> newRecord) throws Exception{
        newRecord.clear();
        List<ListenableFuture<Boolean>> futures=new ArrayList<>();
        try {
            Map<Integer,Throwable> exMap=new HashMap<>();
            for (int i = 0; i < segment.getSelectColumns().size(); i++) {
                Calculator calculator = caPool.borrowObject();
                calculator.clear();
                calculator.setValueParts(segment.getSelectColumns().get(i));
                calculator.setInputRecord(inputRecord);
                calculator.setOutputRecord(newRecord);
                calculator.setSegment(segment);
                ListenableFuture<Boolean> future = pool.submit(new CalculatorCallable(calculator));
                Futures.addCallback(future, new FutureCallback<Boolean>() {
                            @Override
                            public void onSuccess(Boolean aBoolean) {
                                caPool.returnObject(calculator);
                            }
                            @Override
                            public void onFailure(Throwable throwable) {
                                caPool.returnObject(calculator);
                                exMap.put(1,throwable);
                            }
                        }
                        ,pool);
                futures.add(future);
            }
            for(ListenableFuture<Boolean> future:futures){
                future.get();
            }
            if(exMap.get(1)!=null){
                throw new GenericException(exMap.get(1).getMessage());
            }
        }catch (Exception ex){
            log.error("{}",ex);
            throw new GenericException(ex);
        }
    }
    public static void closePool(){
        if(!ObjectUtils.isEmpty(caPool)) {
            caPool.close();
        }
    }

}
