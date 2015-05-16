package com.dchm.configloader;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class LoadProperty extends ConfigProperty {
	private Properties		prop;
	private static Logger	log	= Logger.getLogger(LoadProperty.class.getName());

	public LoadProperty() {
		prop = new Properties();
	}

	@Override
	public boolean loadConfig() {
		this.dataPath = prop.getProperty("data");
		this.hdfsPath = prop.getProperty("HDFS");
		this.hdPath = prop.getProperty("hdPath");
		this.pearsonInputPath = prop.getProperty("pearsonInput");
		this.pearsonOutputPath = prop.getProperty("pearsonOutput");
		this.trainNormal = prop.getProperty("trainNormal");
		this.trainWarning = prop.getProperty("trainWarning");
		this.testCase = prop.getProperty("testSet");
		this.dataRealtime = prop.getProperty("dataRealTime");
		this.trainSpark = prop.getProperty("trainSpark");
		this.testSpark = prop.getProperty("testSpark");
		this.trainDiskSpark = prop.getProperty("trainDiskSpark");
		this.testDiskSpark = prop.getProperty("testDiskSpark");

		if (this.dataPath.length() == 0 || this.hdfsPath.length() == 0
				|| this.hdPath.length() == 0) {
			log.warn("Please fill mainPath & hdfsPath");
			return false;
		}
		return true;
	}

	@Override
	public boolean initConfig(String path) {
		InputStream inputStream = null;
		try {
			inputStream = new FileInputStream(path);
			prop.load(inputStream);
			PropertyConfigurator.configure(prop);
			return true;
		} catch (FileNotFoundException e) {
			log.error("File not found exception ", e);
			return false;
		} catch (IOException e) {
			log.error("IO exception", e);
			return false;
		}
	}

}
