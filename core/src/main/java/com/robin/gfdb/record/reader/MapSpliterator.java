package com.robin.gfdb.record.reader;

import com.robin.rapidoffice.exception.ExcelException;


import java.util.Map;
import java.util.Spliterator;
import java.util.function.Consumer;

public class MapSpliterator implements Spliterator<Map<String,Object>> {
    IDataFileReader reader;
    public MapSpliterator(IDataFileReader reader){
        this.reader=reader;
    }


    @Override
    public boolean tryAdvance(Consumer<? super Map<String, Object>> action) {
        try {
            if (reader.hasNext()) {
                action.accept(reader.next());
                return true;
            } else {
                return false;
            }
        } catch (Exception e) {
            throw new ExcelException(e);
        }
    }

    @Override
    public Spliterator<Map<String, Object>> trySplit() {
        return null;
    }

    @Override
    public long estimateSize() {
        return Long.MAX_VALUE;
    }

    @Override
    public int characteristics() {
        return DISTINCT | IMMUTABLE | NONNULL | ORDERED;
    }
}
