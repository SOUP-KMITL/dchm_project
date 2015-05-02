package com.dchm.Naive;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import scala.Tuple2;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.regex.Pattern;

/**
 * Created by apirat on 5/3/15 AD.
 */
public class NaiveSparkFunction {

    public static JavaRDD<LabeledPoint> ParseStringToLabeledPoint(JavaRDD<String> data){
        final Pattern COMMA	= Pattern.compile(",");
        final Pattern SPACE	= Pattern.compile(" ");
        JavaRDD<LabeledPoint> ret = data.map(new Function<String, LabeledPoint>() {
            @Override
            public LabeledPoint call(String line) throws Exception {
                String[] parts = COMMA.split(line);
                double y = Double.parseDouble(parts[0]);
                String[] tok = SPACE.split(parts[1]);
                double[] x = new double[tok.length - 1];
                for (int i = 1; i < tok.length; ++i) {
                    x[i - 1] = Double.parseDouble(tok[i]);
                }
                return new LabeledPoint(y, Vectors.dense(x));
            }
        });

        return ret;
    }

    public static JavaPairRDD<Double, Double> predictTest(
            JavaRDD<LabeledPoint> data, final NaiveBayesModel model) {
        JavaPairRDD<Double, Double> ret = data
                .mapToPair(new PairFunction<LabeledPoint, Double, Double>() {
                    @Override
                    public Tuple2<Double, Double> call(LabeledPoint p) {
                        return new Tuple2<Double, Double>(model.predict(p
                                .features()), p.label());
                    }
                });
        return ret;
    }

    public static double checkAccuracy(JavaPairRDD<Double, Double> input,
                                       JavaRDD<LabeledPoint> data) {
        double accuracy = input.filter(
                new Function<Tuple2<Double, Double>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<Double, Double> pl) {
                        return pl._1().equals(pl._2());
                    }
                }).count()
                / (double) data.count();
        return accuracy;
    }

    public static JavaRDD<Tuple3<String, ArrayList<String>, ArrayList<Double>>> predictUnknown(
            JavaRDD<Tuple3<String, ArrayList<String>, ArrayList<Vector>>> data,
            final NaiveBayesModel model) {
        JavaRDD<Tuple3<String, ArrayList<String>, ArrayList<Double>>> ret = data
                .map(new Function<Tuple3<String, ArrayList<String>, ArrayList<Vector>>, Tuple3<String,
                        ArrayList<String>, ArrayList<Double>>>() {

                    @Override
                    public Tuple3<String, ArrayList<String>, ArrayList<Double>> call(
                            Tuple3<String, ArrayList<String>, ArrayList<Vector>> line)
                            throws Exception {
                        if (line == null)
                            return null;
                        else {
                            ArrayList<Double> list = new ArrayList<Double>();
                            for (Vector v : line._3()) {
                                if (v == null)
                                    return null;
                                list.add(model.predict(v));
                            }
                            return new Tuple3<String, ArrayList<String>, ArrayList<Double>>(
                                    line._1(), line._2(), list);
                        }
                    }

                });
        return ret;
    }

}
