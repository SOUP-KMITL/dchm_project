package com.dchm.fileio;

import org.apache.hadoop.fs.FileStatus;

import java.io.File;
import java.util.ArrayList;

/**
 * Created by apirat on 5/2/15 AD.
 *
 * Written by MoCca
 *
 */
public interface HadoopDoAble {
	/**
	 * Copy file to HDFS
	 * 
	 * @param file
	 *            file to copy
	 * @param path
	 *            path of file
	 */
	public boolean copyFileToHDFS(File file, String path);

	/**
	 * Check file is exist
	 * 
	 * @param path
	 *            path of file
	 * @return if file is exist return true else return false
	 */
	public boolean fileIsExist(String path);

	/**
	 * List file in directory
	 * 
	 * @param path
	 *            path of directory
	 * @return array list of file
	 */
	public ArrayList<FileStatus> listDirectory(String path);

	/**
	 * read text file from HDFS
	 * 
	 * @param path
	 *            path of file
	 * @return array list of string
	 */
	public ArrayList<String> readFileFromHDFS(String path);

	/**
	 * Create Log file
	 * 
	 * @param path
	 *            path of log file
	 * @return if success return true else return false
	 */

	public boolean createLogFile(String path);
}
