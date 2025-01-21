package com.robin.gfdb.cloud;

import com.google.common.collect.MapMaker;
import com.robin.core.base.util.Const;
import com.robin.core.fileaccess.meta.DataCollectionMeta;
import com.robin.gfdb.hdfs.HDFSProperty;
import com.robin.gfdb.hdfs.HDFSUtil;
import com.robin.gfdb.storage.AbstractFileSystem;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Singleton HDFS FileSystem Accessor,using defaultName as key
 */
public class HdfsFileSystem extends AbstractFileSystem {
	private static final Logger logger=LoggerFactory.getLogger(HdfsFileSystem.class);
	private ThreadLocal<HDFSUtil> hdfsLocal=new ThreadLocal<>();


	public HdfsFileSystem(){
		this.identifier= Const.FILESYSTEM.HDFS.getValue();
	}

	@Override
	public void init(DataCollectionMeta meta) {
		super.init(meta);
		HDFSProperty property=new HDFSProperty();
		property.setHaConfigByObj(meta.getResourceCfgMap());
		HDFSUtil util=new HDFSUtil(property);
		hdfsLocal.set(util);
	}

	@Override
	public List<String> listPath(String sourcePath) throws IOException {
		return null;
	}

	@Override
	public boolean isDirectory(String sourcePath) throws IOException {
		return false;
	}

	@Override
	public Pair<BufferedReader,InputStream> getInResourceByReader(String resourcePath)
			throws IOException {
		HDFSUtil util=hdfsLocal.get();
		InputStream stream=util.getHDFSDataByRawInputStream(resourcePath);
		return Pair.of(getReaderByPath(resourcePath, stream, metaLocal.get().getEncode()),stream);
	}
	
	@Override
	public Pair<BufferedWriter,OutputStream> getOutResourceByWriter(String resourcePath)
			throws IOException {
		HDFSUtil util=hdfsLocal.get();
		OutputStream outputStream=null;
		try {
			if (util.exists(resourcePath)) {
				logger.error("output file {}  exist!,remove it" ,resourcePath );
				util.delete(resourcePath);
			}
			outputStream=util.getHDFSRawOutputStream(resourcePath);
			return Pair.of(getWriterByPath(resourcePath, outputStream, metaLocal.get().getEncode()),outputStream);
		}catch (Exception ex){
			throw new IOException(ex);
		}
	}
	@Override
	public OutputStream getOutResourceByStream(String resourcePath)
			throws IOException {
		HDFSUtil util=hdfsLocal.get();
		try {
			if (util.exists(resourcePath)) {
				logger.error("output file {} exist!,remove it" , resourcePath);
				util.delete(resourcePath);
			}
			return getOutputStreamByPath(resourcePath, util.getHDFSDataByOutputStream(resourcePath));
		}catch (Exception ex){
			throw new IOException(ex);
		}
	}

	@Override
	public OutputStream getRawOutputStream(String resourcePath) throws IOException {
		HDFSUtil util=hdfsLocal.get();
		try {
			if (util.exists(resourcePath)) {
				logger.error("output file {}  exist!,remove it" , resourcePath);
				util.delete(resourcePath);
			}
			return util.getHDFSRawOutputStream(resourcePath);
		}catch (Exception ex){
			throw new IOException(ex);
		}
	}

	@Override
	public InputStream getRawInputStream(String resourcePath) throws IOException {
		HDFSUtil util=hdfsLocal.get();
		try {
			if (util.exists(resourcePath)) {
				return util.getHDFSDataByRawInputStream(resourcePath);
			}else{
				throw new IOException("path "+resourcePath+" not found");
			}
		}catch (Exception ex){
			throw new IOException(ex);
		}
	}

	@Override
	public InputStream getInResourceByStream(String resourcePath)
			throws IOException {
		HDFSUtil util=hdfsLocal.get();
		try {
			if (util.exists(resourcePath)) {
				logger.error("output file {}  exist!,remove it" , resourcePath );
				util.delete(resourcePath);
			}
			return getInputStreamByPath(resourcePath, util.getHDFSDataByInputStream(resourcePath));
		}catch (Exception ex){
			throw new IOException(ex);
		}
	}


	@Override
	public boolean exists(String resourcePath) throws IOException {
		HDFSUtil util=hdfsLocal.get();
		try {
			return util.exists(resourcePath);
		}catch (Exception ex){
			throw new IOException(ex);
		}
	}

	@Override
	public long getInputStreamSize(String resourcePath) throws IOException {
		HDFSUtil util=hdfsLocal.get();
		try {
			if (util.exists(resourcePath)) {
				return util.getHDFSFileSize(resourcePath);
			}else{
				throw new IOException("path "+resourcePath+" not found");
			}
		}catch (Exception ex){
			throw new IOException(ex);
		}
	}
}
