package com.dchm.fileIO;

import org.apache.hadoop.fs.FileStatus;

import java.io.File;
import java.util.ArrayList;

/**
 * Created by apirat on 5/2/15 AD.
 */
public interface HadoopDoAble {
    public boolean copyFileToHDFS(File file, String path);

    public boolean fileIsExist(String path);

    public ArrayList<FileStatus> listDirectory(String path);

    public ArrayList<String> readFileFromHDFS(String path);

    public boolean createLogFile(String path);
}
