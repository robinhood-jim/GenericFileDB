package com.robin.gfdb.record.reader;

import com.robin.core.base.util.Const;
import com.robin.core.convert.util.ConvertUtil;
import com.robin.core.fileaccess.meta.DataCollectionMeta;
import com.robin.core.fileaccess.meta.DataSetColumnMeta;
import com.robin.gfdb.storage.AbstractFileSystem;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;

import java.io.IOException;
import java.util.Map;

@Slf4j
public class CsvFileReader extends AbstractFileReader implements IDataFileReader{
    protected String readLineStr = null;
    protected String split = ",";

    public CsvFileReader(DataCollectionMeta colmeta, AbstractFileSystem fileSystem) {
        super(colmeta, fileSystem);
        setIdentifier(Const.FILEFORMATSTR.CSV.getValue());
        useBufferedReader=true;
    }

    @Override
    public Map<String, Object> pullNext() {
        cachedValue.clear();
        try {
            if (reader != null) {
                readLineStr = reader.readLine();
                if (!ObjectUtils.isEmpty(readLineStr)) {
                    String[] arr = StringUtils.split(readLineStr, split.charAt(0));
                    if (arr.length >= colmeta.getColumnList().size()) {
                        for (int i = 0; i < colmeta.getColumnList().size(); i++) {
                            DataSetColumnMeta meta = colmeta.getColumnList().get(i);
                            cachedValue.put(meta.getColumnName(), ConvertUtil.convertStringToTargetObject(arr[i], meta, formatter));
                        }
                    }
                }
            }
        } catch (IOException ex) {
            log.error("{}", ex.getMessage());
        }
        return cachedValue;
    }
    public void setSplit(String split){
        this.split=split;
    }
}
