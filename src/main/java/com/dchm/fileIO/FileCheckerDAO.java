package com.dchm.fileIO;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.Observer;

/**
 * Created by apirat on 5/2/15 AD.
 *
 * Written by MoCca
 *
 */
public class FileCheckerDAO extends FileChecker {
    private JavaSparkContext ctx;
    private HadoopIO hadoopIO;

    private final String LOG_FILENAME = "dchm_logging";
    private final String PERF_FILENAME = "PerformanceQuery";

    public FileCheckerDAO(JavaSparkContext ctx, String inputPath, String hdfsURL, Configuration
            conf) {
        this.ctx = ctx;
        this.inputPath = inputPath;
        this.outputPath = outputPath;
        this.hdfsURL = hdfsURL;
        this.hadoopIO = new HadoopIODAO(conf);
        this.observers = new ArrayList<Observer>();
        checkLogFile();
    }

    @Override
    public FileStatus getCurrentFile() {
        return this.currentFile;
    }

    @Override
    public void setCurrentFile(FileStatus currentFile) {
        this.currentFile = currentFile;
        currentFileHasChanged();
    }

    private void currentFileHasChanged() {
        setChanged();
        notifyObservers(this);
    }

    public void checkLogFile() {
        if(hadoopIO.fileIsExist(inputPath + LOG_FILENAME)) {
        } else {
            hadoopIO.createLogFile(inputPath + LOG_FILENAME);
            System.out.println("Creating Log file");
        }
    }

    public ArrayList<FileStatus> hasFileNotReading() {
        ArrayList<FileStatus> fileStatuses = listDirectory(inputPath + PERF_FILENAME);
        ArrayList<String> hasRead = hadoopIO.readFileFromHDFS(inputPath + LOG_FILENAME);

        ArrayList<FileStatus> notReadingFiles = new ArrayList<FileStatus>();
        long compareFileModTime = 0;
        if(!hasRead.isEmpty()) {
            compareFileModTime = Long.parseLong(hasRead.get(0));
        }
        for(FileStatus f : fileStatuses) {
            if(f.getModificationTime() > compareFileModTime) {
                notReadingFiles.add(f);
            }
        }
        return notReadingFiles;
    }

    public ArrayList<FileStatus> listDirectory(String path) {
        ArrayList<FileStatus> fileStatuses = hadoopIO.listDirectory(path);
        return fileStatuses == null ? new ArrayList<FileStatus>() : fileStatuses;
    }

    public String getNotReadingFile() {
        return null;
    }

}
