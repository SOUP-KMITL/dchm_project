package com.dchm.performance;

import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.fs.FileStatus;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

import com.dchm.fileIO.HadoopIO;
import com.dchm.fileIO.HadoopIODAO;

/**
 * Created by apirat on 5/16/15 AD.
 */
public class FindPerformance {
    private JavaSparkContext ctx;
    private HadoopIO hadoopIO;
    private String dataPath;
    private int perfID;
    private int percent;
    private int interval;
    private final String DATA_PATH = "/user/mocca/vcenter_data/PerformanceQuery/";

    public FindPerformance(JavaSparkContext ctx, HadoopIODAO hadoopIO, String dataPath){
        this.ctx = ctx;
        this.hadoopIO = hadoopIO;
        this.dataPath = dataPath + DATA_PATH;
    }


    public void run() {
        JavaRDD<String> file;
        JavaPairRDD<String, Integer> data;
        ArrayList<FileStatus> fileStatuses = hadoopIO.listDirectory(this.dataPath);
        HashMap<String, Integer> result = new HashMap<String, Integer>();
        file = ctx.textFile(fileStatuses.get(fileStatuses.size() - 1).getPath().toString());
        data = PerformanceSparkFunction.filterData(file, this.percent, this.perfID, this.interval);
        for(Tuple2<String, Integer> a : data.collect()) {
            if(a == null) continue;
            System.out.println("name=" + a._1() + "\t" + "percent=" + a._2());
        }
    }

    public int getPercent() {
        return percent;
    }

    public void setPercent(int percent) {
        this.percent = percent;
    }

    public int getPerfID() {
        return perfID;
    }

    public void setPerfID(int perfID) {
        this.perfID = perfID;
    }

    public int getInterval() {
        return interval;
    }

    public void setInterval(int interval) {
        this.interval = interval;
    }
}
