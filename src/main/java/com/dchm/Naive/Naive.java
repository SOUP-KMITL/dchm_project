package com.dchm.Naive;

import org.apache.hadoop.fs.FileStatus;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.classification.NaiveBayesModel;

/**
 * Created by apirat on 5/3/15 AD.
 */
public abstract class Naive {
    protected FileStatus        currentFile;
    protected JavaSparkContext  ctx;
    protected NaiveBayesModel   model;

    protected String		    hdfsPath;
    protected String			dataPath;
    protected String			trainPath;
    protected String			testPath;

    public abstract void test();

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
