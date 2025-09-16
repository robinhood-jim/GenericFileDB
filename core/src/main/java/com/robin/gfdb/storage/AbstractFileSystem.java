/*
 * Copyright (c) 2015,robinjim(robinjim@126.com)
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.robin.gfdb.storage;

import com.robin.core.compress.util.CompressDecoder;
import com.robin.core.compress.util.CompressEncoder;
import com.robin.core.fileaccess.meta.DataCollectionMeta;
import org.springframework.util.ObjectUtils;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * abstract resource system access Utils (Local/Hdfs/ApacheVFS(including ftp sftp)/S3/Tencent cloud/aliyun)
 */
public abstract class AbstractFileSystem implements IFileSystem,Closeable {
	protected String identifier;
	protected DataCollectionMeta colmeta;
	protected Charset charset= StandardCharsets.UTF_8;


	protected static BufferedReader getReaderByPath(String path, InputStream  in, Charset encode) throws IOException{
		return new BufferedReader(new InputStreamReader(getInputStreamByPath(path,in),encode));
	}
	protected static InputStream getInputStreamByPath(String path, InputStream  in) throws IOException{
		return CompressDecoder.getInputStreamByCompressType(path,in);
	}
	protected static OutputStream getOutputStreamByPath(String path, OutputStream out) throws IOException{
		return CompressEncoder.getOutputStreamByCompressType(path,out);
	}
	protected static BufferedWriter getWriterByPath(String path, OutputStream out, String encode) throws IOException{
		return new BufferedWriter(new OutputStreamWriter(getOutputStreamByPath(path,out),encode));
	}


	
	@Override
	public void init(DataCollectionMeta meta){
		this.colmeta=meta;
		if(!ObjectUtils.isEmpty(colmeta.getEncode())){
			charset=Charset.forName(colmeta.getEncode());
		}
	}
	public void finishWrite(OutputStream outputStream) {

	}
	@Override
	public String getIdentifier() {
		return identifier;
	}

	@Override
	public void close() throws IOException {

	}
}
