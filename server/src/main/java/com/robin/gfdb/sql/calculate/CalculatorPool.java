package com.robin.gfdb.sql.calculate;

import com.robin.core.base.exception.GenericException;
import lombok.extern.slf4j.Slf4j;

import stormpot.Pool;

import stormpot.Timeout;

import java.io.Closeable;
import java.util.concurrent.TimeUnit;

@Slf4j
public class CalculatorPool implements Closeable {
    private Pool<Calculator> pool;
    private CalculatorAllocator allocator;
    public CalculatorPool(){
        allocator=new CalculatorAllocator();
        //Config<Calculator> config = new Config<Calculator>().setSize(40).setAllocator(allocator);
        pool = Pool.from(allocator).setSize(40).build();

    }
    public void close() {
        pool.shutdown();
    }
    public Calculator borrowObject() {
        try {
            return pool.claim(new Timeout(1,TimeUnit.SECONDS));
        }catch (InterruptedException ex){
            log.error("{}",ex.getMessage());
            throw new GenericException(ex);
        }
    }
    public void returnObject(Calculator calculator){
        calculator.release();
    }


}
