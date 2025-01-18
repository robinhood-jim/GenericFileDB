package com.robin.gfdb.record.reader;

import com.robin.core.base.util.Const;
import com.robin.core.convert.util.ConvertUtil;
import com.robin.core.fileaccess.meta.DataCollectionMeta;
import com.robin.core.fileaccess.meta.DataSetColumnMeta;
import com.robin.gfdb.storage.AbstractFileSystem;
import lombok.extern.slf4j.Slf4j;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
@Slf4j
public class XmlFileReader extends AbstractFileReader {
    private XMLInputFactory factory;
    private XMLStreamReader streamReader;
    private String rooteleName;
    private String entityName;
    private boolean secondContainEntity=false;
    public XmlFileReader(DataCollectionMeta colmeta, AbstractFileSystem fileSystem) {
        super(colmeta, fileSystem);
        setIdentifier(Const.FILEFORMATSTR.XML.getValue());
    }

    @Override
    public void init() throws IOException {
        super.init();
        try{
            factory=XMLInputFactory.newFactory();
            factory.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, Boolean.FALSE);
            factory.setProperty(XMLInputFactory.SUPPORT_DTD, Boolean.FALSE);
            if(inputStream!=null) {
                streamReader=factory.createXMLStreamReader(inputStream,colmeta.getEncode());
            }
            while(streamReader.hasNext()){
                streamReader.next();
                if(streamReader.getEventType()== XMLStreamConstants.START_ELEMENT){
                    if(rooteleName==null){
                        rooteleName=streamReader.getLocalName();
                    }else if(entityName==null){
                        //is second layout entityName?
                        if(streamReader.getAttributeCount()>0){
                            entityName=streamReader.getLocalName();
                            secondContainEntity=true;
                            break;
                        }
                        entityName=streamReader.getLocalName();
                    }else{
                        break;
                    }
                }
            }
        }catch(Exception ex){
            ex.printStackTrace();
        }
    }

    @Override
    public Map<String, Object> pullNext() {
        boolean finishget=false;
        String column=null;
        String value=null;
        StringBuilder builder=new StringBuilder();
        try{
            cachedValue.clear();
            while (streamReader.getEventType()!=XMLStreamConstants.END_DOCUMENT && streamReader.hasNext() && streamReader.getEventType()!=XMLStreamConstants.END_ELEMENT) {
                if(streamReader.getEventType()==XMLStreamConstants.START_ELEMENT){
                    String curName=streamReader.getLocalName();
                    if(!secondContainEntity){
                        if(!curName.equals(entityName)){
                            if(finishget) {
                                break;
                            }
                            //contain attribute
                            if(streamReader.getAttributeCount()>0){
                                Map<String,Object> tmap=new HashMap<>();
                                for (int i=0;i<streamReader.getAttributeCount();i++) {
                                    column=streamReader.getAttributeName(i).getLocalPart();
                                    value=streamReader.getAttributeValue(i);
                                    adjustColumn(column,value,tmap);
                                }
                                //value
                                streamReader.next();
                                getValue(builder);
                                tmap.put("value",builder.toString());
                                cachedValue.put(curName,tmap);
                            }
                        }
                    }else{
                        if(finishget) {
                            break;
                        }
                        if(curName.equals(entityName)){
                            int count=streamReader.getAttributeCount();
                            for (int i=0;i<count;i++){
                                column=streamReader.getAttributeName(i).getLocalPart();
                                value=streamReader.getAttributeValue(i);
                                adjustColumn(column,value,cachedValue);
                            }
                        }
                    }
                }else if(streamReader.getEventType()==XMLStreamConstants.END_ELEMENT){
                    String curName=streamReader.getLocalName();
                    if(curName.equals(entityName)){
                        finishget=true;
                    }
                }
                streamReader.next();
            }
        }catch(Exception ex){
            log.error("{}",ex.getMessage());
        }
        return cachedValue;
    }
    private void adjustColumn(String sourceColumnName,String value,Map<String,Object> retmap) {
        String column=sourceColumnName;
        if(!columnMap.containsKey(sourceColumnName)){
            if(columnMap.containsKey(sourceColumnName.toLowerCase())){
                column=sourceColumnName.toLowerCase();
            }else if(columnMap.containsKey(sourceColumnName.toUpperCase())){
                column=sourceColumnName.toUpperCase();
            }
        }
        DataSetColumnMeta meta= columnMap.get(column);
        if(meta!=null) {
            retmap.put(column, ConvertUtil.convertStringToTargetObject(value, meta, formatter));
        }
    }
    private void getValue(StringBuilder builder) throws Exception{
        if(builder.length()>0){
            builder.delete(0,builder.length());
        }
        while(streamReader.getEventType()==XMLStreamConstants.CHARACTERS){
            builder.append(streamReader.getText());
            streamReader.next();
        }
    }
}
