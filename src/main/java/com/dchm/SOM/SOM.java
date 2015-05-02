package com.dchm.SOM;

import com.dchm.base.CalculateAble;
import org.apache.hadoop.fs.FileStatus;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Observer;

/**
 * Created by apirat on 5/3/15 AD.
 */
public abstract class SOM implements Observer, CalculateAble {
    protected FileStatus currentFile;
    protected JavaSparkContext ctx;

    public FileStatus getCurrentFile() {
        return currentFile;
    }

    public void setCurrentFile(FileStatus currentFile) {
        this.currentFile = currentFile;
    }
}
