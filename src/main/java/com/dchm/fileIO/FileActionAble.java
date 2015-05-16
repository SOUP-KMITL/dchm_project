package com.dchm.fileIO;

import org.apache.hadoop.fs.FileStatus;

import java.io.File;
import java.util.ArrayList;

/**
 * Created by apirat on 5/2/15 AD.
 *
 * Written by MoCca
 *
 */
public interface FileActionAble {
	/**
	 * List file in directory
	 * 
	 * @param path
	 *            path of directory
	 * @return array list of file
	 */
	public ArrayList<FileStatus> listDirectory(String path);

	/**
	 * Get get file is not reading
	 * 
	 * @return file name
	 */
	public String getNotReadingFile();

	/**
	 * check file is not read
	 * 
	 * @return array list of file name
	 */
	public ArrayList<FileStatus> hasFileNotReading();

}
