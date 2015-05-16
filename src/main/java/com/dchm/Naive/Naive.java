package com.dchm.naive;

import com.dchm.base.CalculateAble;
import com.dchm.fileio.HadoopIO;
import org.apache.hadoop.fs.FileStatus;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.codehaus.jettison.json.JSONObject;

import java.io.File;
import java.util.ArrayList;
import java.util.Observer;

/**
 * Created by apirat on 5/3/15 AD.
 *
 * Written by MoCca
 *
 */
public abstract class Naive implements Observer, CalculateAble {
	protected FileStatus currentFile;
	protected JavaSparkContext ctx;
	protected NaiveBayesModel model;
	protected HadoopIO hadoopIO;

	protected String hdfsPath;
	protected String dataPath;
	protected String trainPath;
	protected String testPath;

	/**
	 * upload file to HDFS
	 * 
	 * @param file
	 *            file to upload
	 */
	protected abstract void upload(File file);

	/**
	 * Write result form naive bayes to file
	 * 
	 * @param input
	 *            result form naive bayes
	 * @param filename Local path file
	 * @return file is write success
	 */
	protected abstract File writeFile(ArrayList<JSONObject> input, Long filename);

	protected abstract void test();

	public FileStatus getCurrentFile() {
		return currentFile;
	}

	public void setCurrentFile(FileStatus currentFile) {
		this.currentFile = currentFile;
	}

	public JavaSparkContext getCtx() {
		return ctx;
	}

	public void setCtx(JavaSparkContext ctx) {
		this.ctx = ctx;
	}

	public NaiveBayesModel getModel() {
		return model;
	}

	public void setModel(NaiveBayesModel model) {
		this.model = model;
	}

	public String getHdfsPath() {
		return hdfsPath;
	}

	public void setHdfsPath(String hdfsPath) {
		this.hdfsPath = hdfsPath;
	}

	public String getDataPath() {
		return dataPath;
	}

	public void setDataPath(String dataPath) {
		this.dataPath = dataPath;
	}

	public String getTrainPath() {
		return trainPath;
	}

	public void setTrainPath(String trainPath) {
		this.trainPath = trainPath;
	}

	public String getTestPath() {
		return testPath;
	}

	public void setTestPath(String testPath) {
		this.testPath = testPath;
	}
}
