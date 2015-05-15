package com.dchm.SOM;

import java.util.ArrayList;
import java.util.Arrays;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;

import scala.Tuple2;

public class SOMFunction {
	public static JavaRDD<String> filterPerfData(JavaRDD<String> data, final String regex) {
        JavaRDD<String> ret = data.filter(new Function<String, Boolean>() {

            @Override
            public Boolean call(String str) throws Exception {
                return str.matches(regex);
            }
        });
        return ret;
    }

    public static Boolean filterLogFile(JavaRDD<String> data, final String regex) {
        JavaRDD<String> ret = data.filter(new Function<String, Boolean>() {

            @Override
            public Boolean call(String str) throws Exception {
                return str.contains(regex);
            }
        });
        if (ret.count() > 0)
            return false;
        else
            return true;
    }

    public static JavaRDD<String> filterVMName(JavaRDD<String> data, final String regex) {
        JavaRDD<String> ret = data.filter(new Function<String, Boolean>() {

            @Override
            public Boolean call(String str) throws Exception {
                return str.matches(regex);
            }
        }).flatMap(new FlatMapFunction<String, String>() {

            @Override
            public Iterable<String> call(String t) throws Exception {
                return Arrays.asList(new JSONObject(t).getString("name")
                        + "@@"
                        + new JSONObject(t).getJSONObject("summary").getJSONObject("vm")
                        .getString("val"));
            }

        });
        return ret;
    }

    public static JavaPairRDD<String, ArrayList<Double[]>> pairData(JavaRDD<String> data, final int size) {
        JavaPairRDD<String, ArrayList<Double[]>> ret = data.mapToPair(new PairFunction<String, String,
                ArrayList<Double[]>>(){
            @Override
            public Tuple2<String, ArrayList<Double[]>> call(String str)
                    throws Exception {
                String[] keep = str.split("@@");
                ArrayList<Double[]> list = new ArrayList<Double[]>();
                list.add(toDouble(keep[1].split(",") , size));
                list.add(toDouble(keep[2].split(",") , size));
                return new Tuple2<String, ArrayList<Double[]>>(keep[0], list);
            }

            public Double[] toDouble(String[] tokens, int size) {
                Double[] ret = new Double[size];
                for (int i = 0; i < size; i++) {
                    ret[i] = Double.parseDouble(tokens[i]);
                }
                return ret;
            }

        });

        return ret;

    }

    public static JavaRDD<String> prepareMap(JavaRDD<String> data) {
        JavaRDD<String> ret = data.flatMap(new FlatMapFunction<String, String>() {

                    @Override
                    public Iterable<String> call(String t) throws Exception {
                        JSONObject jObj = new JSONObject(t);
                        JSONArray jArray = jObj.getJSONArray("value");
                        int counterId;
                        String name = jObj.getJSONObject("entity")
                                .getString("val");
                        String receive = "", transfer = "";
                        for (int i = 0; i < jArray.length(); i++) {
                            jObj = jArray.getJSONObject(i);
                            counterId = jObj.getJSONObject("id").getInt(
                                    "counterId");
                            if (counterId == 148) {
                                receive = jObj.getString("value");
                            } else if (counterId == 149) {
                                transfer = jObj.getString("value");
                            }
                        }
                        return Arrays.asList(name
                                + "@@"
                                + receive
                                + "@@"
                                + transfer);
                    }

                }).filter(new Function<String, Boolean>() {
                    @Override
                    public Boolean call(String line) throws Exception {
                        return line.split("@@").length == 3 ? true : false;
                    }
                });
        return ret;
    }
    public static JavaRDD<Integer> prepareData(JavaRDD<String> data){
        JavaRDD<Integer> size = data.flatMap(new FlatMapFunction<String, Integer>() {
                    @Override
                    public Iterable<Integer> call(String in) throws Exception {
                        int size = in.split("@@")[1].split(",").length;
                        return Arrays.asList(size);
                    }
                }).sortBy(new Function<Integer, Integer>() {

            @Override
            public Integer call(Integer value) throws Exception {
                return value;
            }
        }, true, 1);
        return size;
    }
}
