package com.robin.gfdb.record.reader;

import com.robin.comm.util.xls.ExcelSheetProp;
import com.robin.core.base.util.Const;
import com.robin.core.fileaccess.meta.DataCollectionMeta;
import com.robin.gfdb.storage.AbstractFileSystem;
import com.robin.rapidoffice.excel.WorkBook;
import com.robin.rapidoffice.excel.WorkSheet;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

public class XlsxFileReader extends AbstractFileReader{

    private Iterator<Map<String,Object>> rowIterator;
    ExcelSheetProp sheetProp;
    WorkBook workBook;
    WorkSheet currentSheet;
    int sheetNum;


    public XlsxFileReader(DataCollectionMeta colmeta, AbstractFileSystem fileSystem) {
        super(colmeta, fileSystem);
        setIdentifier(Const.FILEFORMATSTR.XLSX.getValue());
    }

    @Override
    public void init() throws IOException {
        super.init();
        try{
            sheetProp=ExcelSheetProp.fromDataCollectionMeta(colmeta);
            if(inputStream!=null) {
                workBook=new WorkBook(inputStream);

                if(workBook.getSheetNum()>0){
                    currentSheet=workBook.getSheet(1).get();
                    sheetNum=1;
                }
                rowIterator=workBook.openMapStream(currentSheet,sheetProp).iterator();
            }

        }catch (Exception ex){

        }

    }

    @Override
    public Map<String, Object> pullNext() {
        try {
            cachedValue.clear();
            if (rowIterator.hasNext()) {
                return rowIterator.next();
            }else{
                if(sheetNum<= workBook.getSheetNum()){
                    sheetNum++;
                    currentSheet=workBook.getSheet(sheetNum).get();
                    rowIterator=workBook.openMapStream(currentSheet,sheetProp).iterator();
                }
                return rowIterator.next();
            }
        }catch (Exception ex){

        }
        return null;
    }

    @Override
    public void close() throws IOException {
        super.close();
        workBook.close();
    }
}
