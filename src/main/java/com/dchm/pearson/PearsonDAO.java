package com.dchm.pearson;

import com.dchm.base.CalculateAble;
import com.dchm.fileIO.FileChecker;
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
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

/**
 * Created by apirat on 5/3/15 AD.
 */
public class PearsonDAO extends Pearson {
    private static Logger log	= Logger.getLogger(PearsonDAO.class.getName());

    public PearsonDAO(JavaSparkContext ctx) {
        this.ctx = ctx;
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
//        long start = System.nanoTime();
//        JavaRDD<String> data = PearsonSparkFunction.prepareMap(input);
//        JSONArray jsonArr = new JSONArray();
//        JavaRDD<Integer> size = PearsonSparkFunction.prepareData(data);
//        if (size.collect().size() >= 2) {
//            JavaPairRDD<String, ArrayList<Double[]>> pair = PearsonSparkFunction.pairData(data,
//                    size.collect().get(0).intValue());
//            int loop = pair.collect().size();
//            // StringBuilder ret = new StringBuilder();
//            List<Tuple2<String, ArrayList<Double[]>>> value = pair.collect();
//            JavaDoubleRDD series1_X, series1_Y, series2_X, series2_Y;
//            Double correlation1, correlation2;
//            for (int i = 0; i < loop; i++) {
//                series1_X = ctx.parallelizeDoubles(new ArrayList<Double>(Arrays.asList(value.get(i)._2().get(0))));
//                series2_X = ctx.parallelizeDoubles(new ArrayList<Double>(Arrays.asList(value.get(i)._2().get(1))));
//
//                for (int j = i + 1; j < loop; j++) {
//                    series1_Y = ctx.parallelizeDoubles(new ArrayList<Double>(Arrays.asList(value.get(j)._2().get(1))));
//                    series2_Y = ctx.parallelizeDoubles(new ArrayList<Double>(Arrays.asList(value.get(j)._2().get(0))));
//                    if (series1_X.toArray().size() != series1_Y.toArray().size() ||
//                            series2_X.toArray().size() != series2_Y.toArray().size()) {
//                            continue;
//                    }
//                    correlation1 = Statistics.corr(series1_X.srdd(), series1_Y.srdd(), "pearson");
//                    correlation2 = Statistics.corr(series2_X.srdd(), series2_Y.srdd(), "pearson");
//
//                    JSONObject json = new JSONObject();
//                    try {
//                        json.put("vm", vm.get(i) + "@" + vm.get(j));
//                        json.put("Tx-Rx", correlation1.toString());
//                        json.put("Rx-Tx", correlation2.toString());
//                        jsonArr.put(json);
//                    } catch (JSONException e) {
//                        log.error("JSON Create in Pearson", e);
//                    }
//                }
//            }
//            long end = System.nanoTime();
//            JSONObject json = new JSONObject();
//            try {
//                json.put("group", dvPortGroup);
//                json.put("correlation", jsonArr);
//            } catch (JSONException e) {
//                log.error("Create JSON ScanDVport", e);
//            }
//            try {
//                System.out.println("write file : " + dvPortGroup);
//                synchronized (tmpFile) {
//                    BufferedWriter bw = new BufferedWriter(new FileWriter(tmpFile.getAbsoluteFile(), true));
//                    bw.write(json.toString().concat("\n"));
//                    bw.flush();
//                    bw.close();
//                }
//            } catch (IOException e) {
//                // TODO Auto-generated catch block
//                e.printStackTrace();
//            }
//        }
    }

}
