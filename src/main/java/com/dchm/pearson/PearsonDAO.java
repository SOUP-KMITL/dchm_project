package com.dchm.pearson;

import com.dchm.base.CalculateAble;
import com.dchm.fileIO.FileChecker;
import com.dchm.fileIO.HadoopIO;
import org.apache.hadoop.fs.FileStatus;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.stat.Statistics;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import scala.Tuple2;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;

/**
 * Created by apirat on 5/3/15 AD.
 *
 * Written By MoCca
 *
 */
public class PearsonDAO extends Pearson {
    private static Logger log	= Logger.getLogger(PearsonDAO.class.getName());
    private final String PEARSON_OUTPUT_PATH = "result/correlation/";
    private final String LOCAL_DATA_PATH = "data";
    private final String LOCAL_DATA_SUBPATH = "Pearson";

    public PearsonDAO(JavaSparkContext ctx, HadoopIO hadoopIO, String hdfsPath, String dataPath) {
        this.ctx = ctx;
        this.hadoopIO = hadoopIO;
        this.hdfsPath = hdfsPath;
        this.dataPath = this.hdfsPath + dataPath;
    }
    /**
     * This method is called whenever the observed object is changed. An
     * application calls an <tt>Observable</tt> object's
     * <code>notifyObservers</code> method to have all the object's
     * observers notified of the change.
     *
     * @param obs   the observable object.
     * @param arg an argument passed to the <code>notifyObservers</code>
     */
    @Override
    public void update(Observable obs, Object arg) {
        this.currentFile = (FileStatus) arg;
        calculate();
    }

    @Override
    public void calculate() {
        String[] name = name = this.currentFile.getPath().toString().split("/");
        Path filePath = null;
        Path folder = Paths.get(this.LOCAL_DATA_PATH, this.LOCAL_DATA_SUBPATH).toAbsolutePath().normalize();
        try {
            folder = Files.createDirectories(folder);
            filePath =folder.resolve(name[name.length-1]);
        } catch (IOException e) {
            log.error("Can't create directory", e);
        }
        JavaRDD<String> data = PearsonSparkFunction.prepareMap(ctx.textFile(this.currentFile.getPath().toString()));
        //JSONArray jsonArr = new JSONArray();
        ArrayList<JSONArray> jsonList;
        JavaRDD<Integer> size = PearsonSparkFunction.prepareData(data);
        if (size.collect().size() >= 2) {
            JavaPairRDD<String, ArrayList<Double[]>> pair = PearsonSparkFunction.pairData(data,
                    size.collect().get(0).intValue());
            int loop = pair.collect().size();
            List<Tuple2<String, ArrayList<Double[]>>> value = pair.collect();
            JavaDoubleRDD series1_X, series1_Y, series2_X, series2_Y;
            Double correlation1, correlation2;
            for (int i = 0; i < loop; i++) {
                series1_X = ctx.parallelizeDoubles(new ArrayList<Double>(Arrays.asList(value.get(i)._2().get(0))));
                series2_X = ctx.parallelizeDoubles(new ArrayList<Double>(Arrays.asList(value.get(i)._2().get(1))));
                jsonList = new ArrayList<JSONArray>();
                for (int j = i + 1; j < loop; j++) {
                    series1_Y = ctx.parallelizeDoubles(new ArrayList<Double>(Arrays.asList(value.get(j)._2().get(1))));
                    series2_Y = ctx.parallelizeDoubles(new ArrayList<Double>(Arrays.asList(value.get(j)._2().get(0))));
                    if (series1_X.collect().size() != series1_Y.collect().size() ||
                            series2_X.collect().size() != series2_Y.collect().size()) {
                            continue;
                    }
                    correlation1 = Statistics.corr(series1_X.srdd(), series1_Y.srdd(), "pearson");
                    correlation2 = Statistics.corr(series2_X.srdd(), series2_Y.srdd(), "pearson");

//                    JSONObject json = new JSONObject();
                    JSONArray jArry = new JSONArray();
                    JSONObject objA = new JSONObject();
                    JSONObject objB = new JSONObject();
                    JSONObject objP = new JSONObject();
                    try {

                        objA.put("name", value.get(i)._1());
                        objA.put("id", "");
                        objA.put("type", "send");
                        objB.put("name", value.get(j)._1());
                        objB.put("id", "");
                        objB.put("type", "receive");
                        objP.put("Pearson", correlation1.toString());
                        jArry.put(objA);
                        jArry.put(objB);
                        jArry.put(objP);
                        jsonList.add(jArry);
                        jArry = new JSONArray();
                        objA = new JSONObject();
                        objB = new JSONObject();
                        objP = new JSONObject();
                        objA.put("name", value.get(i)._1());
                        objA.put("id", "");
                        objA.put("type", "receive");
                        objB.put("name", value.get(j)._1());
                        objB.put("id", "");
                        objB.put("type", "send");
                        objP.put("Pearson", correlation2.toString());
                        jArry.put(objA);
                        jArry.put(objB);
                        jArry.put(objP);
                        jsonList.add(jArry);
//                        json.put("vm", value.get(i)._1() + "@" + value.get(j)._1());
//                        json.put("Tx-Rx", correlation1.toString());
//                        json.put("Rx-Tx", correlation2.toString());
                    } catch (JSONException e) {
                        log.error("JSON Create in Pearson", e);
                    }
                }
                writeFile(jsonList, filePath);
            }
            upload(filePath.toFile());
        }
    }

    @Override
    protected void upload(File file) {
        this.hadoopIO.copyFileToHDFS(file, this.dataPath + PEARSON_OUTPUT_PATH);
        file.delete();
    }

    /**
     *
     * Written by shadowslight
     *
     * @param input     JSON list to write into file
     * @param filePath  Local Path file
     */

    @Override
    protected void writeFile(ArrayList<JSONArray> input, Path filePath) {
        try {
            BufferedWriter bw = Files.newBufferedWriter(filePath,
                    StandardCharsets.UTF_8, StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND);
            for (JSONArray j : input) {
                bw.write(j.toString() + "\n");
            }
            bw.flush();
            bw.close();
        } catch (IOException e) {
            log.error("Write file in NaiveDAO has error", e);
        }
    }
}
