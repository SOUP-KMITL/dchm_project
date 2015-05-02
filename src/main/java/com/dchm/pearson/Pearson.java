package com.dchm.pearson;

import org.apache.hadoop.fs.FileStatus;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by apirat on 5/3/15 AD.
 */
public abstract class Pearson {
    protected FileStatus currentFile;
    protected JavaSparkContext ctx;

    public FileStatus getCurrentFile() {
        return currentFile;
    }

    public void setCurrentFile(FileStatus currentFile) {
        this.currentFile = currentFile;
    }
}
