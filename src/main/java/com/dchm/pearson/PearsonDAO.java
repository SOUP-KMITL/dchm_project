package com.dchm.pearson;

import com.dchm.base.CalculateAble;
import com.dchm.fileIO.FileChecker;
import org.apache.hadoop.fs.FileStatus;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Observable;
import java.util.Observer;

/**
 * Created by apirat on 5/3/15 AD.
 */
public class PearsonDAO extends Pearson implements Observer, CalculateAble {

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
        System.out.println("PearsonDAO : File has Changed so RECALCULATED");
    }
}
