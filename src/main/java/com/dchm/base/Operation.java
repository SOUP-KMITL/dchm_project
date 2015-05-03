package com.dchm.base;

import com.dchm.Naive.Naive;
import com.dchm.Naive.NaiveDAO;
import com.dchm.SOM.SOM;
import com.dchm.SOM.SOMDAO;
import com.dchm.configLoader.ConfigProperty;
import com.dchm.configLoader.LoadProperty;
import com.dchm.fileIO.FileChecker;
import com.dchm.fileIO.FileCheckerDAO;
import com.dchm.fileIO.HadoopIO;
import com.dchm.fileIO.HadoopIODAO;
import com.dchm.pearson.Pearson;
import com.dchm.pearson.PearsonDAO;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.File;
import java.nio.file.FileSystems;
import java.nio.file.Path;

/**
 * Created by apirat on 5/2/15 AD.
 *
 * Written by MoCca
 *
 */
public class Operation {
    private ConfigProperty configLoader;
    private FileChecker fileChecker;

    public Operation(String[] args) {
        configLoader = new LoadProperty();
        loadConfigFile(configLoader, args);
    }

    /**
     * Load config.properties file to set value.
     * if it has argument so load from that.
     * if it has not. load default path.
     *
     * @param config    ConfigProperty Class.
     * @param args      Parameters from command line.
     */

    public void loadConfigFile(ConfigProperty config, String args[]) {
        if (args.length < 1) {
            System.err.println("Load default path");
            Path configPath = FileSystems.getDefault().getPath(
                    new File(System.getProperty("user.dir")).getParent(),
                    "config", "config.properties");
            System.err.println(configPath.toString());

            if (!configLoader.initConfig(configPath.toString())) {
                System.exit(-1);
            }
        } else {
            if (!configLoader.initConfig(args[0])) {
                System.exit(-1);
            }
        }
        if (!configLoader.loadConfig()) {
            System.exit(-1);
        }
    }

    public void run() {
        SparkConf sparkConf = new SparkConf().setAppName("JAVASPARK");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", configLoader.getHdfsPath());


        fileChecker = new FileCheckerDAO(ctx, configLoader.getHdPath(), configLoader.getHdfsPath(), conf);

        Pearson pearson = new PearsonDAO(ctx, new HadoopIODAO(conf), configLoader.getHdfsPath(),
                configLoader.getHdPath());
        SOM som = new SOMDAO(ctx);
        Naive naive = new NaiveDAO(ctx, new HadoopIODAO(conf), configLoader.getHdfsPath(),
                configLoader.getTrainSpark(), configLoader.getTestSpark(), configLoader.getHdPath());

        fileChecker.registerObserver(pearson);
        fileChecker.registerObserver(som);
        fileChecker.registerObserver(naive);

        for(FileStatus f : fileChecker.hasFileNotReading()) {
            System.out.println(f.getPath().toString() + "\t" + f.getModificationTime());
            this.fileChecker.setCurrentFile(f);
        }

        System.out.println("Hello, World");
    }


}
