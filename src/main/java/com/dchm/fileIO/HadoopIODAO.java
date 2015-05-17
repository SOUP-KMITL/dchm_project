package com.dchm.fileIO;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

/**
 * Created by apirat on 5/2/15 AD.
 *
 * Written by MoCca
 *
 */
public class HadoopIODAO extends HadoopIO {
	private static Logger log = Logger.getLogger(HadoopIODAO.class.getName());

	public HadoopIODAO(Configuration conf) {
		this.conf = conf;
	}

	public void deleteFile(String path) {
		try {
			fs = FileSystem.get(conf);
			fs.delete(new Path(path), true);
			fs.close();
		} catch (IOException e) {
			log.error("Can't delete file", e);
		}
	}

	public boolean copyFileToHDFS(File file, String path) {
		try {
			fs = FileSystem.get(conf);
			Path hdPath;
			if (file.isDirectory()) {
				for (File f : file.listFiles()) {
					hdPath = new Path(path + f.getName());
					if (pathExist(hdPath)) {
						log.error("File Exist Can't Copy");
						continue;
					}
					fs.copyFromLocalFile(new Path(f.getPath()), hdPath);
				}
				fs.close();
				return true;
			} else {
				hdPath = new Path(path + file.getName());
				if (pathExist(hdPath)) {
					log.error("File Exist Can't Copy");
					return false;
				}
				fs.copyFromLocalFile(new Path(file.getPath()), hdPath);
				fs.close();
				return true;
			}
		} catch (IOException e) {
			log.error("Error in copyFileToHadoop", e);
			return false;
		}
	}

	/**
	 * Check path
	 * 
	 * @param path
	 *            path to check
	 * @return if path is exist return true else return false
	 * @throws IOException
	 */

	private boolean pathExist(Path path) throws IOException {
		return fs.exists(path) ? true : false;
	}

	public boolean fileIsExist(String path) {
		try {
			FileSystem fs = FileSystem.get(conf);
			Path pt = new Path(path);
			if (fs.exists(pt)) {
				fs.close();
				return true;
			} else {
				fs.close();
				return false;
			}
		} catch (IOException e) {
			return false;
		}
	}

	public ArrayList<FileStatus> listDirectory(String path) {
		ArrayList<FileStatus> ret;
		try {
			ret = new ArrayList<FileStatus>();
			fs = FileSystem.get(conf);
			for (FileStatus status : fs.listStatus(new Path(path))) {
				ret.add(status);
			}
			fs.close();
			return ret;
		} catch (IOException e) {
			log.error("List Directory Error", e);
			return new ArrayList<FileStatus>();
		}
	}

	public ArrayList<String> readFileFromHDFS(String path) {
		ArrayList<String> ret = new ArrayList<String>();
		try {
			fs = FileSystem.get(conf);
			BufferedReader bufferedReader = new BufferedReader(
					new InputStreamReader(fs.open(new Path(path))));
			String line = bufferedReader.readLine();
			while (line != null) {
				ret.add(line);
				line = bufferedReader.readLine();
			}
			fs.close();
		} catch (IOException e) {
			log.error("ReadFileFromHDFS exception : ", e);
		}
		return ret;
	}

	@Override
	public boolean createLogFile(String path) {
		try {
			FileSystem fs = FileSystem.get(conf);
			Path pt = new Path(path);
			System.out.println("Creating : " + path);
			if (!fs.exists(pt)) {
				fs.createNewFile(pt);
			}
			fs.close();
			return true;
		} catch (IOException e) {
			log.error("Init Logging fail", e);
			return false;
		}
	}

}
