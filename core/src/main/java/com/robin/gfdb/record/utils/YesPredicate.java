package com.robin.gfdb.record.utils;

import org.apache.parquet.filter2.predicate.Statistics;
import org.apache.parquet.filter2.predicate.UserDefinedPredicate;

import java.io.Serializable;

public class YesPredicate extends UserDefinedPredicate<Integer> implements Serializable {

    @Override
    public boolean keep(Integer integer) {
        return true;
    }

    @Override
    public boolean canDrop(Statistics<Integer> statistics) {
        return false;
    }

    @Override
    public boolean inverseCanDrop(Statistics<Integer> statistics) {
        return false;
    }
}
