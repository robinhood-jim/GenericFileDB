package com.robin.gfdb.sql.calculate;

import java.util.concurrent.Callable;

public class CalculatorCallable implements Callable<Boolean> {
    private Calculator calculator;


    public CalculatorCallable(Calculator calculator){
        this.calculator=calculator;
    }

    @Override
    public Boolean call() throws Exception {
        calculator.setBusyTag(true);

        return calculator.doCalculate();
    }
}
