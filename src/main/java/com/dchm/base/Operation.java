package com.dchm.base;

import java.io.File;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import com.dchm.Naive.Naive;
import com.dchm.Naive.NaiveDAO;
import com.dchm.SOM.SOM;
import com.dchm.SOM.SOMDAO;
import com.dchm.configLoader.ConfigProperty;
import com.dchm.configLoader.LoadProperty;
import com.dchm.fileIO.FileChecker;
import com.dchm.fileIO.FileCheckerDAO;
import com.dchm.fileIO.HadoopIODAO;
import com.dchm.pearson.Pearson;
import com.dchm.pearson.PearsonDAO;
import com.dchm.performance.FindPerformance;

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

    private void printCmd() {
        System.out.println("------ command -------");
        System.out.println("1. Find Status");
        System.out.println("2. RealTime Monitoring");
        System.out.println("0. exit");
    }

    private void runMonitoring(JavaSparkContext ctx, Configuration conf) {
        fileChecker = new FileCheckerDAO(ctx, configLoader.getHdPath(), configLoader.getHdfsPath(), conf);

        Pearson pearson = new PearsonDAO(ctx, new HadoopIODAO(conf), configLoader.getHdfsPath(),
                configLoader.getHdPath());
        SOM som = new SOMDAO(ctx, new HadoopIODAO(conf), configLoader.getHdfsPath(),
                configLoader.getHdPath());
        Naive naive = new NaiveDAO(ctx, new HadoopIODAO(conf), configLoader.getHdfsPath(),
                configLoader.getTrainSpark(), configLoader.getTestSpark(), configLoader.getHdPath());

        fileChecker.registerObserver(pearson);
        fileChecker.registerObserver(som);
        fileChecker.registerObserver(naive);

        for (FileStatus f : fileChecker.hasFileNotReading()) {
            System.out.println(f.getPath().toString() + "\t" + f.getModificationTime());
            this.fileChecker.setCurrentFile(f);
            fileChecker.updateLogFile(f.getModificationTime() + "");
        }
    }

    private void findStatus(JavaSparkContext ctx, Configuration conf) {
        String dataPath = configLoader.getHdfsPath();
        FindPerformance perf = new FindPerformance(ctx, new HadoopIODAO(conf), dataPath);
        Scanner sc = new Scanner(System.in);
        int input;
        System.out.print("1. Find CPU\n2. Find Memory\nEnter : ");
        if (!sc.hasNextInt()) {
            System.out.print("Invalid input type. Try again : ");
            sc.next();
            return;
        }
        input = sc.nextInt();
        switch (input) {
            case 1 :
                perf.setPerfID(2);
                break;
            case 2 :
                perf.setPerfID(24);
                break;
        }
        System.out.print("Enter percent : ");
        if (!sc.hasNextInt()) {
            System.out.print("Invalid input type. Try again : ");
            sc.next();
            return;
        }
        input = sc.nextInt();
        perf.setPercent(input);
        System.out.print("Enter interval [0-86400] : ");
        if (!sc.hasNextInt()) {
            System.out.print("Invalid input type. Try again : ");
            sc.next();
            return;
        }
        input = sc.nextInt();
        if(input < 0 || input > 86400) {
            System.out.println("Input is out of range");
            return;
        }
        perf.setInterval(input);
        perf.run();
    }

    public void run() {
        Scanner sc = new Scanner(System.in);
        SparkConf sparkConf = new SparkConf().setAppName("JAVASPARK");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", configLoader.getHdfsPath());

        boolean runFlag = true;
        int input;

        while (runFlag) {
            printCmd();
            if (!sc.hasNextInt()) {
                System.out.print("Invalid input type. Try again : ");
                sc.next();
                continue;
            }
            input = sc.nextInt();
            switch (input) {
                case 0 :
                    runFlag = false;
                    break;
                case 1 :
                    findStatus(ctx, conf);
                    break;
                case 2 :
                    runMonitoring(ctx, conf);
                    break;
            }

        }

    }


}
