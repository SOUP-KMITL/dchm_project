package com.dchm.fileIO;

import org.apache.hadoop.fs.FileStatus;

import java.util.ArrayList;
import java.util.Observable;
import java.util.Observer;

/**
 * Created by apirat on 5/2/15 AD.
 *
 * Written by MoCca
 *
 */
public abstract class FileChecker extends Observable implements FileActionAble {
    protected String inputPath;
    protected String outputPath;
    protected String hdfsURL;
    protected FileStatus currentFile;
    protected ArrayList<Observer> observers;

    public ArrayList<Observer> getObservers() {
        return observers;
    }

    public void setObservers(ArrayList<Observer> observers) {
        this.observers = observers;
    }

    public void registerObserver(Observer observer) {
        observers.add(observer);
    }

    public void removeObserver(Observer observer) {
        observers.remove(observer);
    }


    /**
     *
     * sequential use for() { ob.update() }
     * parallel use thread.run()
     *
     * @param observable    all observer to notify.
     *
     */
    public void notifyObservers(Observable observable) {
        System.out.println("Notify to PearsonDAO and NaiveDAO that \"File has Changed\"");
        for (Observer ob : observers) {
            ob.update(observable,this.currentFile);
        }
    }

    public abstract FileStatus getCurrentFile();

    public abstract void setCurrentFile(FileStatus currentFile);

    public abstract void updateLogFile(String path);

    public void setInputPath(String inputPath) {
        this.inputPath = inputPath;
    }

    public void setOutputPath(String outputPath) {
        this.outputPath = outputPath;
    }

    public void setHdfsURL(String hdfsURL) {
        this.hdfsURL = hdfsURL;
    }

    public String getInputPath() {
        return inputPath;
    }

    public String getOutputPath() {
        return outputPath;
    }

    public String getHdfsURL() {
        return hdfsURL;
    }
}
