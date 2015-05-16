package com.dchm.Naive;

import com.dchm.fileIO.HadoopIO;

import java.io.File;

/**
 * Created by apirat on 5/3/15 AD.
 *
 * Written by MoCca
 *
 */
public abstract class FindImpact {
	protected String folder;
	protected int numberOfFilePearson;
	protected int percentage = 50;
	protected String vmName;
	protected String dataPath;
	protected HadoopIO hadoopIO;

	public String getVmName() {
		return vmName;
	}

	public void setVmName(String vmName) {
		this.vmName = vmName;
	}

	public abstract void run();

	/**
	 * upload file to HDFS
	 * 
	 * @param file
	 *            file to upload
	 */

	protected abstract void upload(File file);

}
