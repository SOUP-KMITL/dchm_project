package com.dchm.fileIO;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

/**
 * Created by apirat on 5/2/15 AD.
 *
 * Written by MoCca
 *
 */
public abstract class HadoopIO implements HadoopDoAble {
	protected FileSystem fs;
	protected Configuration conf;

	/**
	 * Get file system
	 * 
	 * @return file system
	 */
	public FileSystem getFs() {
		return fs;
	}

	/**
	 * Set file system
	 * 
	 * @param fs
	 *            file system
	 */

	public void setFs(FileSystem fs) {
		this.fs = fs;
	}

	/**
	 * Get configuration
	 * 
	 * @return configuration
	 */

	public Configuration getConf() {
		return conf;
	}

	/**
	 * Set configuration
	 * 
	 * @param conf
	 *            configuration
	 */

	public void setConf(Configuration conf) {
		this.conf = conf;
	}

	/**
	 * Delete file in HDFS
	 * 
	 * @param path
	 *            path of delete file
	 */

	public abstract void deleteFile(String path);
}
