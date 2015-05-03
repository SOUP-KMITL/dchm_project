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
    public ArrayList<FileStatus> listDirectory(String path);

    public String getNotReadingFile();

    public ArrayList<FileStatus> hasFileNotReading();

}
