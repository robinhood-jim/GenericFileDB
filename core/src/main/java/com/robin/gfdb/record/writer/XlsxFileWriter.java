package com.robin.gfdb.record.writer;

import com.robin.comm.util.xls.ExcelSheetProp;
import com.robin.core.base.util.Const;
import com.robin.core.fileaccess.meta.DataCollectionMeta;
import com.robin.gfdb.storage.AbstractFileSystem;
import com.robin.rapidoffice.excel.WorkBook;
import com.robin.rapidoffice.excel.WorkSheet;
import org.springframework.util.ObjectUtils;

import java.io.IOException;
import java.util.Map;

public class XlsxFileWriter extends AbstractFileWriter implements IDataFileWriter{
    private int maxRecNum=1_048_575;
    public static final String MAX_RECORD_NUM_COLUMN="maxRecNumColumn";
    private int currentRecNum=0;
    ExcelSheetProp sheetProp;
    WorkBook workBook;
    WorkSheet currentSheet;
    int sheetNum;

    protected XlsxFileWriter(DataCollectionMeta colmeta, AbstractFileSystem fileSystem) {
        super(colmeta, fileSystem);
        setIdentifier(Const.FILEFORMATSTR.XLSX.getValue());
    }

    @Override
    public void initalize() throws IOException {
        super.initalize();
        if (!ObjectUtils.isEmpty(colmeta.getResourceCfgMap().get(MAX_RECORD_NUM_COLUMN))) {
            maxRecNum = Integer.parseInt(colmeta.getResourceCfgMap().get(MAX_RECORD_NUM_COLUMN).toString());
        }
        try{
            sheetProp=ExcelSheetProp.fromDataCollectionMeta(colmeta);
            if(outputStream!=null) {
                workBook=new WorkBook(outputStream);

                if(workBook.getSheetNum()>0){
                    currentSheet=workBook.getSheet(1).get();
                    sheetNum=1;
                }
                currentSheet=workBook.createSheet("sheet"+sheetNum,sheetProp);
            }
        }catch (Exception ex){

        }
    }

    @Override
    public void writeRecord(Map<String, Object> map) throws IOException {
        currentRecNum++;
        if(currentRecNum>=maxRecNum){
            currentSheet.finish();
            sheetNum++;
            currentSheet=workBook.createSheet("sheet"+sheetNum,sheetProp);
        }
        currentSheet.writeRow(map);
    }

    @Override
    public void finishWrite() throws IOException {
        if(currentSheet!=null) {
            currentSheet.finish();
        }
        workBook.close();
    }
}
