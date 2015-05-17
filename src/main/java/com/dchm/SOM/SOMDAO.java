package com.dchm.SOM;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Observable;

import org.apache.hadoop.fs.FileStatus;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.codehaus.jettison.json.JSONException;

import scala.Tuple2;

import com.dchm.fileIO.HadoopIO;

/**
 * Created by apirat on 5/3/15 AD.
 */
public class SOMDAO extends SOM {
	private static Logger log = Logger.getLogger(SOMDAO.class.getName());
	private final String SOM_OUTPUT_PATH = "result/som/";
	private final String LOCAL_DATA_PATH = "data";
	private final String LOCAL_DATA_SUBPATH = "SOM";
	private static final double DECAY_RATE = 0.99;
	private static final double MIN_ALPHA = 0.01;
	private double alpha = 0.8;

	public SOMDAO(JavaSparkContext ctx, HadoopIO hadoopIO, String hdfsPath, String dataPath) {
        this.ctx = ctx;
        this.hadoopIO = hadoopIO;
        this.hdfsPath = hdfsPath;
        this.dataPath = this.hdfsPath + dataPath;
	}

	/**
	 * This method is called whenever the observed object is changed. An
	 * application calls an <tt>Observable</tt> object's
	 * <code>notifyObservers</code> method to have all the object's observers
	 * notified of the change.
	 *
	 * @param obs
	 *            the observable object.
	 * @param arg
	 *            an argument passed to the <code>notifyObservers</code>
	 */
	@Override
	public void update(Observable obs, Object arg) {
		this.currentFile = (FileStatus) arg;
		calculate();
	}

	@Override
	public void calculate() {
		System.out.println("SOM : File has Changed so RECALCULATED");
		String[] name = name = this.currentFile.getPath().toString().split("/");
        Path filePath = null;
        Path folder = Paths.get(this.LOCAL_DATA_PATH, this.LOCAL_DATA_SUBPATH).toAbsolutePath().normalize();
        try {
            folder = Files.createDirectories(folder);
            filePath =folder.resolve(name[name.length-1]);
        } catch (IOException e) {
            log.error("Can't create directory", e);
        }
        JavaRDD<String> data = SOMFunction.prepareMap(ctx.textFile(this.currentFile.getPath().toString()));
        JavaRDD<Integer> size = SOMFunction.prepareData(data);
        if (size.collect().size() >= 2) {
            JavaPairRDD<String, ArrayList<Double[]>> pair = SOMFunction.pairData(data,
                    size.collect().get(0).intValue());
            List<Tuple2<String, ArrayList<Double[]>>> value = pair.collect();
            SOMCalculate somCal = new SOMCalculate(value.size()*2, value.size()*2, alpha, MIN_ALPHA, DECAY_RATE, Arrays.asList(value.get(0)._2().get(0)).size());
            somCal.Train(value);
            String result = null;
            try {
            	result = somCal.Test(value);
            	writeFile(result, filePath);
            	upload(filePath.toFile());
			} catch (JSONException e) {
				e.printStackTrace();
			}
        }
	}

	@Override
	protected void upload(File file) {
		this.hadoopIO.copyFileToHDFS(file, this.dataPath + SOM_OUTPUT_PATH);
        file.delete();
	}

	@Override
	protected void writeFile(String result, Path filePath) {
		try {
			BufferedWriter bw = Files.newBufferedWriter(filePath,
			        StandardCharsets.UTF_8, StandardOpenOption.CREATE,
			        StandardOpenOption.APPEND);
			bw.write(result);
			bw.flush();
            bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
}
