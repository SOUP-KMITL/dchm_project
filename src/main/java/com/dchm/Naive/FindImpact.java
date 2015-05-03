package com.dchm.Naive;

import com.dchm.fileIO.HadoopIO;

/**
 * Created by apirat on 5/3/15 AD.
 *
 * Written by MoCca
 *
 */
public abstract class FindImpact {
    protected String folder;
    protected int numberOfFilePearson;
    protected int percentage = 50;
    protected String vmName;
    protected HadoopIO hadoopIO;

    public String getVmName() {
        return vmName;
    }

    public void setVmName(String vmName) {
        this.vmName = vmName;
    }

    public abstract void run();


}
