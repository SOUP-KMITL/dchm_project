package com.dchm.SOM;

import com.dchm.base.CalculateAble;
import com.dchm.fileIO.HadoopIO;

import org.apache.hadoop.fs.FileStatus;
import org.apache.spark.api.java.JavaSparkContext;
import org.codehaus.jettison.json.JSONArray;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Observer;

/**
 * Created by apirat on 5/3/15 AD.
 */
public abstract class SOM implements Observer, CalculateAble {
    protected FileStatus currentFile;
    protected JavaSparkContext ctx;
    protected HadoopIO          hadoopIO;

    protected String		    hdfsPath;
    protected String			dataPath;
    
    protected abstract void upload(File file);

    protected abstract void writeFile(String result, Path filePath);

    public FileStatus getCurrentFile() {
        return currentFile;
    }

    public void setCurrentFile(FileStatus currentFile) {
        this.currentFile = currentFile;
    }
    
    public String getDataPath() {
        return dataPath;
    }

    public void setDataPath(String dataPath) {
        this.dataPath = dataPath;
    }

    public String getHdfsPath() {
        return hdfsPath;
    }

    public void setHdfsPath(String hdfsPath) {
        this.hdfsPath = hdfsPath;
    }
}
