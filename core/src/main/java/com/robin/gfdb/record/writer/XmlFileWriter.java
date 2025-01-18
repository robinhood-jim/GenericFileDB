package com.robin.gfdb.record.writer;

import com.robin.core.base.util.Const;
import com.robin.core.fileaccess.meta.DataCollectionMeta;
import com.robin.gfdb.storage.AbstractFileSystem;
import org.tukaani.xz.FinishableOutputStream;

import javax.xml.stream.XMLEventFactory;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.io.IOException;
import java.util.Map;

public class XmlFileWriter extends AbstractFileWriter{
    XMLOutputFactory factory;
    XMLEventFactory ef = XMLEventFactory.newInstance();
    XMLStreamWriter streamWriter;
    protected XmlFileWriter(DataCollectionMeta colmeta, AbstractFileSystem fileSystem) {
        super(colmeta, fileSystem);
        setIdentifier(Const.FILEFORMATSTR.XML.getValue());
        useBufferedWriter=true;
    }

    @Override
    public void initalize() throws IOException {
        super.initalize();
        try {
            factory=XMLOutputFactory.newFactory();
            streamWriter=factory.createXMLStreamWriter(writer);
            ef.createStartDocument(colmeta.getEncode(),"1.0");
            streamWriter.writeStartDocument(colmeta.getEncode(),"1.0");
            streamWriter.writeCharacters("\n");
            streamWriter.writeStartElement("records");
            streamWriter.writeCharacters("\n");
        }catch (Exception ex){
            throw new IOException(ex);
        }
    }

    @Override
    public void writeRecord(Map<String, Object> map) throws IOException {
        try {
            streamWriter.writeCharacters("\t");
            streamWriter.writeStartElement("record");
            for (int i = 0; i < colmeta.getColumnList().size(); i++) {
                String name = colmeta.getColumnList().get(i).getColumnName();
                String value=getOutputStringByType(map,name);
                if(value!=null){
                    streamWriter.writeAttribute(name,value);
                }
            }
            streamWriter.writeEndElement();
            streamWriter.writeCharacters("\n");
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    @Override
    public void finishWrite() throws IOException {
        try{
            streamWriter.writeEndElement();
            if(!(outputStream instanceof FinishableOutputStream)) {
                streamWriter.flush();
                writer.flush();
            }else{
                ((FinishableOutputStream) outputStream).finish();
            }
        }catch (Exception ex){
            throw new IOException(ex);
        }
    }

    @Override
    public void flush() throws IOException {
        try {
            if(!FinishableOutputStream.class.isAssignableFrom(outputStream.getClass())) {
                streamWriter.flush();
                outputStream.flush();
            }
        }catch (Exception ex){

        }
    }

    @Override
    public void close() throws IOException {
        try{
            streamWriter.close();
        }catch (XMLStreamException ex){

        }
        doClose();
    }
}
