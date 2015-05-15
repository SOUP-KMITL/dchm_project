package com.dchm.SOM;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import scala.Tuple2;

public class SOMCalculate {
	private int mVectors;
	private int mVecLen;
	private int maxClusters;
	private double minAlpha;
	private double mAlpha;
	private double decayRate;
	private int mIterations;
	private double D[];

	private double w[][];
	
	public SOMCalculate(int numVectors, int Maximum_Clusters, double Alpha_Start,
			double Minimum_Alpha, double Decay_Rate, int Vector_Length) {
		mVectors = numVectors;
		mVecLen = Vector_Length;
		mAlpha = Alpha_Start;
		minAlpha = Minimum_Alpha;
		decayRate = Decay_Rate;
		mIterations = 0;
		maxClusters = Maximum_Clusters;
		D = new double[maxClusters];
		w = new double[maxClusters][mVecLen];
		for (int i = 0; i < maxClusters; i++) {
			for (int j = 0; j < mVecLen; j++) {
				w[i][j] = new Random().nextDouble();
			}
		}
		return;
	}
	
	public void Train(List<Tuple2<String, ArrayList<Double[]>>> VMdata) {
		int Iterations = 0;
		int i;
		int VecNum;
		int DMin;

		while (mAlpha > minAlpha) {
			Iterations += 1;
			for (VecNum = 0; VecNum < mVectors; VecNum++) {
				List<Double> networkData;
				if(VecNum%2==0){
					networkData = Arrays.asList(VMdata.get(VecNum/2)._2().get(0));
				}else{
					networkData = Arrays.asList(VMdata.get(VecNum/2)._2().get(1));
				}
				ComputeInput(networkData);
				DMin = Minimum(D);
				for (i = 0; i < mVecLen; i++) {
					w[DMin][i] = w[DMin][i]
							+ (mAlpha * (networkData.get(i) - w[DMin][i]));
				}
			}
			mAlpha = decayRate * mAlpha;
		}
		mIterations = Iterations;
		return;
	}

	public String Test(List<Tuple2<String, ArrayList<Double[]>>> VMdata) throws JSONException {
		int VecNum;
		int DMin;
		JSONObject objA;
		StringBuilder data = new StringBuilder();
		for (VecNum = 0; VecNum < mVectors; VecNum++) {
			List<Double> networkData;
			if(VecNum%2==0){
				networkData = Arrays.asList(VMdata.get(VecNum/2)._2().get(0));
			}else{
				networkData = Arrays.asList(VMdata.get(VecNum/2)._2().get(1));
			}
			objA = new JSONObject();
			ComputeInput(networkData);
			DMin = Minimum(D);
//			objA.put("name", vmNetworkList.get((VecNum / 2)).getName());
			objA.put("id", VMdata.get(VecNum/2)._1());
			if (VecNum % 2 == 0) {
				objA.put("type", "receive");
				objA.put("category", DMin);
			} else {
				objA.put("type", "send");
				objA.put("category", DMin);
			}
			data.append(objA);
			data.append('\n');
		}
		return data.toString();
	}

	private void ComputeInput(List<Double> Training_Tests) {
		int i, j;
		for (i = 0; i < D.length; i++) {
			D[i] = 0.0;
		}
		for (i = 0; i < maxClusters; i++) {
			for (j = 0; j < mVecLen; j++) {
				D[i] += Math.pow((w[i][j] - Training_Tests.get(j)), 2);
			}
		}
		return;
	}

	private int Minimum(double D[]) {
		int winner = 0;
		double min = D[0];
		for (int i = 0; i < maxClusters; i++) {
			if (D[i] < min) {
				min = D[i];
				winner = i;
			}
		}
		return winner;
	}

	public int Iterations() {
		return mIterations;
	}
}
