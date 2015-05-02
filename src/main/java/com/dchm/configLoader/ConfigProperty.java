package com.dchm.configLoader;


public abstract class ConfigProperty implements LoadConfig {
	protected String	dataPath;
	protected String	hdfsPath;
	protected String	hdPath;
	protected String	pearsonInputPath;
	protected String	pearsonOutputPath;
	protected String	trainNormal;
	protected String	trainWarning;
	protected String	testCase;
	protected String	dataRealtime;
	protected String	trainSpark;
	protected String	testSpark;
	protected String	trainDiskSpark;
	protected String	testDiskSpark;

	public String getTrainDiskSpark() {
		return trainDiskSpark;
	}

	public void setTrainDiskSpark(String trainDiskSpark) {
		this.trainDiskSpark = trainDiskSpark;
	}

	public String getTestDiskSpark() {
		return testDiskSpark;
	}

	public void setTestDiskSpark(String testDiskSpark) {
		this.testDiskSpark = testDiskSpark;
	}

	public String getTestSpark() {
		return testSpark;
	}

	public void setTestSpark(String testSpark) {
		this.testSpark = testSpark;
	}

	public String getTrainSpark() {
		return trainSpark;
	}

	public void setTrainSpark(String trainSpark) {
		this.trainSpark = trainSpark;
	}

	public String getDataRealtime() {
		return dataRealtime;
	}

	public void setDataRealtime(String dataRealtime) {
		this.dataRealtime = dataRealtime;
	}

	public String getTrainNormal() {
		return trainNormal;
	}

	public void setTrainNormal(String trainNormal) {
		this.trainNormal = trainNormal;
	}

	public String getTrainWarning() {
		return trainWarning;
	}

	public void setTrainWarning(String trainWarning) {
		this.trainWarning = trainWarning;
	}

	public String getTestCase() {
		return testCase;
	}

	public void setTestCase(String testCase) {
		this.testCase = testCase;
	}

	public abstract boolean loadConfig();

	public abstract boolean initConfig(String path);

	public String getDataPath() {
		return dataPath;
	}

	public void setDataPath(String dataPath) {
		this.dataPath = dataPath;
	}

	public String getHdfsPath() {
		return hdfsPath;
	}

	public void setHdfsPath(String hdfsPath) {
		this.hdfsPath = hdfsPath;
	}

	public String getHdPath() {
		return hdPath;
	}

	public void setHdPath(String hdPath) {
		this.hdPath = hdPath;
	}

	public String getPearsonInputPath() {
		return pearsonInputPath;
	}

	public void setPearsonInputPath(String pearsonInputPath) {
		this.pearsonInputPath = pearsonInputPath;
	}

	public String getPearsonOutputPath() {
		return pearsonOutputPath;
	}

	public void setPearsonOutputPath(String pearsonOutputPath) {
		this.pearsonOutputPath = pearsonOutputPath;
	}

}
