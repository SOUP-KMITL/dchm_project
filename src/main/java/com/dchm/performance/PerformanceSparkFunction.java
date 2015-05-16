package com.dchm.performance;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import scala.Tuple2;

/**
 * Created by apirat on 5/16/15 AD.
 */
public class PerformanceSparkFunction {
    public static JavaPairRDD<String, Integer> filterData(JavaRDD<String> data, final int percent, final int perfID,
                                                          final int interval) {
        JavaPairRDD<String, Integer> ret = data.mapToPair(new PairFunction<String, String, Integer>() {

            @Override
            public Tuple2<String, Integer> call(String line) throws Exception {
                String sampleCSV;
                int maxValue = 0;
                JSONObject json = new JSONObject(line);
                JSONArray jsonArr = new JSONArray();
                sampleCSV = json.getString("sampleInfoCSV");
                sampleCSV = sampleCSV.split(",")[0];

                String vmname = json.getJSONObject("entity").getString("val");
                jsonArr = json.getJSONArray("value");
                String vPerf = null;
                for(int i=0; i < jsonArr.length(); i++) {
                    if (jsonArr.getJSONObject(i).getJSONObject("id").getInt("counterId") == perfID) {
                        vPerf = jsonArr.getJSONObject(i).getString("value");
                    }
                }

                if (vPerf != null) {
                    int period = interval / Integer.parseInt(sampleCSV);
                    String[] tokenizer = vPerf.split(",");
                    int tmpValue = 0;
                    for (int i = tokenizer.length - 1; i >= tokenizer.length - period; i--) {

                        tmpValue = Integer.parseInt(tokenizer[i]);
                        if (tmpValue > maxValue && tmpValue > percent * 100) {
                            maxValue = tmpValue;
                        }
                    }
                    return new Tuple2<String, Integer>(vmname, maxValue);
                }
                return new Tuple2<String, Integer>(vmname, -1);
            }
        });
        return ret;
    }
}
