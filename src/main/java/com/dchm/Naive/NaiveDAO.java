package com.dchm.Naive;

import com.dchm.base.CalculateAble;
import com.dchm.fileIO.FileChecker;
import com.dchm.fileIO.HadoopIO;
import org.apache.hadoop.fs.FileStatus;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.classification.NaiveBayes;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import scala.Tuple3;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Observable;
import java.util.Observer;
import java.util.regex.Pattern;

/**
 * Created by apirat on 5/3/15 AD.
 */
public class NaiveDAO extends Naive {
    private static Logger   log	= Logger.getLogger(NaiveDAO.class.getName());
    private HadoopIO hadoopIO;

    private final String    OUTPUT_PATH = "result/naive/";

    public NaiveDAO(JavaSparkContext ctx, HadoopIO hadoopIO, String hdfsPath,  String trainPath, String testPath, String
            dataPath){
        this.ctx = ctx;
        this.hadoopIO = hadoopIO;
        this.hdfsPath = hdfsPath;
        this.trainPath = this.hdfsPath + trainPath;
        this.testPath = this.hdfsPath + testPath;
        this.dataPath = this.hdfsPath + dataPath;
        System.out.println(this.dataPath);
        JavaRDD<LabeledPoint> training = NaiveSparkFunction.ParseStringToLabeledPoint(ctx.textFile(trainPath));
        this.model = NaiveBayes.train(training.rdd(), 1.0);
        test();
    }

    @Override
    public void test() {
        JavaRDD<String> dataTest = ctx.textFile(this.testPath);
        JavaRDD<LabeledPoint> testing = NaiveSparkFunction.ParseStringToLabeledPoint(dataTest);

        JavaPairRDD<Double, Double> predictionAndLabel = NaiveSparkFunction
                .predictTest(testing, model);

        double accuracy = NaiveSparkFunction.checkAccuracy(predictionAndLabel,
                testing);
        System.out.println("Accuracy = " + accuracy);
    }

    static class PreProcessing implements
            Function<String, Tuple3<String, ArrayList<String>, ArrayList<Vector>>> {
        private static final Pattern	COMMA	= Pattern.compile(",");

        @Override
        public Tuple3<String, ArrayList<String>, ArrayList<Vector>> call(String line)
                throws Exception {
            if (line == null) {
                return null;
            }
            JSONObject json = new JSONObject(line);
            String vm = json.getString("vm");
            double cpu_value, mem_value, tx_value, rx_value, cpu_change, mem_change, tx_change, rx_change;
            ArrayList<String> time = new ArrayList<String>();
            String[] cpu, mem, tx, rx, timeArray;

            timeArray = COMMA.split(json.getString("sampleInfoCSV"));
            cpu = COMMA.split(json.getString("cpu"));
            mem = COMMA.split(json.getString("mem"));
            tx = COMMA.split(json.getString("tx"));
            rx = COMMA.split(json.getString("rx"));
            ArrayList<Vector> ret = new ArrayList<Vector>();
            Vector tmpVector;
            double[] value;
            for (int i = 4; i < cpu.length; i++) {
                value = new double[8];
                cpu_value = Double.parseDouble(cpu[i]);
                mem_value = Double.parseDouble(mem[i]);
                tx_value = Double.parseDouble(tx[i]);
                rx_value = Double.parseDouble(rx[i]);
                cpu_change = cpu_value - Double.parseDouble(cpu[i - 4]);
                mem_change = mem_value - Double.parseDouble(mem[i - 4]);
                tx_change = tx_value - Double.parseDouble(tx[i - 4]);
                rx_change = rx_value - Double.parseDouble(rx[i - 4]);

                /********* CPU *********/
                if (cpu_value > 7500) {
                    cpu_value = 3;
                } else if (cpu_value <= 7500 && cpu_value > 5000) {
                    cpu_value = 2;
                } else if (cpu_value <= 5000 && cpu_value > 2500) {
                    cpu_value = 1;
                } else {
                    cpu_value = 0;
                }

                if (cpu_change <= 250 && cpu_change >= -250) {
                    cpu_change = 1;
                } else {
                    if (cpu_change > 0)
                        cpu_change = 2;
                    else if (cpu_change < 0)
                        cpu_change = 0;
                    else
                        cpu_change = 1;
                }

                /********* MEM *********/
                if (mem_value > 7500) {
                    mem_value = 3;
                } else if (mem_value <= 7500 && mem_value > 5000) {
                    mem_value = 2;
                } else if (mem_value <= 5000 && mem_value > 2500) {
                    mem_value = 1;
                } else {
                    mem_value = 0;
                }

                if (mem_change <= 250 && mem_change >= -250) {
                    mem_change = 1;
                } else {
                    if (mem_change > 0)
                        mem_change = 2;
                    else if (mem_change < 0)
                        mem_change = 0;
                    else
                        mem_change = 1;
                }

                /********* TX *********/
                double log10;
                log10 = Math.log10(tx_value);
                if (log10 <= 1.0) {
                    tx_value = 0;
                } else if (log10 > 1.0 && log10 <= 2.5) {
                    tx_value = 1;
                } else if (log10 > 2.5 && log10 <= 3.0) {
                    tx_value = 2;
                } else if (log10 > 3.0) {
                    tx_value = 3;
                }

                if (tx_change <= 5.0 && tx_change >= -5.0) {
                    tx_change = 1;
                } else {
                    if (tx_change > 0) {
                        tx_change = 2;
                    } else if (tx_change < 0) {
                        tx_change = 0;
                    } else {
                        tx_change = 1;
                    }
                }

                /********* RX *********/
                log10 = Math.log10(rx_value);
                if (log10 <= 1.0) {
                    rx_value = 0;
                } else if (log10 > 1.0 && log10 <= 2.5) {
                    rx_value = 1;
                } else if (log10 > 2.5 && log10 <= 3.0) {
                    rx_value = 2;
                } else if (log10 > 3.0) {
                    rx_value = 3;
                }

                if (rx_change <= 5.0 && rx_change >= -5.0) {
                    rx_change = 1;
                } else {
                    if (rx_change > 0) {
                        rx_change = 2;
                    } else if (rx_change < 0) {
                        rx_change = 0;
                    } else {
                        rx_change = 1;
                    }
                }
                time.add(timeArray[(i * 2) + 1]);
                value[0] = cpu_value;
                value[1] = mem_value;
                value[2] = tx_value;
                value[3] = rx_value;
                value[4] = cpu_change;
                value[5] = mem_change;
                value[6] = tx_change;
                value[7] = rx_change;
                ret.add(Vectors.dense(value));
            }
            return new Tuple3<String, ArrayList<String>, ArrayList<Vector>>(vm, time, ret);
        }
    }

    static class GetData implements Function<String, String> {

        @Override
        public String call(String line) throws Exception {
            JSONObject ret = new JSONObject();
            String cpu = "", mem = "", tx = "", rx = "";
            JSONObject json = new JSONObject(line);
            ret.put("vm", json.getJSONObject("entity").getString("val"));
            ret.put("sampleInfoCSV", json.getString("sampleInfoCSV"));
            JSONArray jArray = json.getJSONArray("value");
            int counterId;
            for (int i = 0; i < jArray.length(); i++) {
                json = jArray.getJSONObject(i);
                counterId = json.getJSONObject("id").getInt("counterId");
                switch (counterId) {
                    case 2:
                        cpu = json.getString("value");
                        break;
                    case 24:
                        mem = json.getString("value");
                        break;
                    case 148:
                        rx = json.getString("value");
                        break;
                    case 149:
                        tx = json.getString("value");
                        break;
                }
            }
            if (cpu.equals("") || mem.equals("") || rx.equals("")
                    || tx.equals("")) {
                return null;
            } else {
                ret.put("cpu", cpu);
                ret.put("mem", mem);
                ret.put("rx", rx);
                ret.put("tx", tx);
                return ret.toString();
            }
        }

    }

    private void upload(File file) {
        this.hadoopIO.copyFileToHDFS(file, this.dataPath + OUTPUT_PATH);
        file.delete();
    }

    private File writeFile(ArrayList<JSONObject> input, Long filename) {
        try {
            File file = new File(filename.toString());
            BufferedWriter bw = new BufferedWriter(new FileWriter(file));
            for (JSONObject j : input) {
                bw.write(j.toString() + "\n");
            }
            bw.close();
            return file;
        } catch (IOException e) {
            log.error("Write file in NaiveDAO has error", e);
            return null;
        }
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
        System.out.println("NaiveDAO : File has Changed so RECALCULATED");
        SimpleDateFormat format = new SimpleDateFormat("dd-MM-yyyy(HH.mm.ss)");
        ArrayList<File> upFile = new ArrayList<File>();
        JavaRDD<String> getData;
        JavaRDD<Tuple3<String, ArrayList<String>, ArrayList<Vector>>> preProcess;

        JavaRDD<Tuple3<String, ArrayList<String>, ArrayList<Double>>> predicted;

        JSONObject json;
        ArrayList<JSONObject> jsonList;
        String[] name;
        Date date = new Date();

        jsonList = new ArrayList<JSONObject>();
        getData = ctx.textFile(this.currentFile.getPath().toString()).map(new GetData()).cache();
        preProcess = getData.map(new PreProcessing()).cache();

        predicted = NaiveSparkFunction.predictUnknown(preProcess, model);

        System.out.println("GetData = " + getData.count());
        System.out.println("PreProcess = " + preProcess.count());
        System.out.println("Predict  = " + predicted.count());

        for (Tuple3<String, ArrayList<String>, ArrayList<Double>> a : predicted.collect()) {
            if (a == null)
                continue;
            for (int j = 0; j < a._3().size(); j++) {
                if (a._3().get(j) == 1) {
                    try {
                        json = new JSONObject();
                        json.put("vm", a._1());
                        json.put("time", a._2().get(j));
                        json.put("status", "warning");
                        jsonList.add(json);
                    } catch (JSONException e) {
                        log.error("JSON Parse Fail", e);
                    }
                }
            }
        }
        name = this.currentFile.getPath().toString().split("/");
        try {
            date = (Date) format.parse(name[name.length - 1]);
        } catch (ParseException e) {
            log.error("Parse Date error", e);
            date = new Date();
        }

        File uploadFile = writeFile(jsonList, date.getTime());
        if(uploadFile != null) {
            upload(uploadFile);
        }
        System.out.println("Done Naive file\n-----------------------");
    }

}
