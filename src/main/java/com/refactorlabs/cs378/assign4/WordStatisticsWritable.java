package com.refactorlabs.cs378.assign4;

/**
 * Created by gvsi on 10/09/2016.
 */
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class WordStatisticsWritable implements Writable {
    private long paragraphCount;

    private double mean;

    private double variance;

    public long getParagraphCount() {
        return paragraphCount;
    }

    public void setParagraphCount(long paragraphCount) {
        this.paragraphCount = paragraphCount;
    }

    public double getMean() {
        return mean;
    }

    public void setMean(double mean) {
        this.mean = mean;
    }

    public double getVariance() {
        return variance;
    }

    public void setVariance(double variance) {
        this.variance = variance;
    }


    public void readFields(DataInput in) throws IOException {
        // Read the data out in the order it is written

        paragraphCount = in.readLong();
        mean = in.readDouble();
        variance = in.readDouble();
    }

    public void write(DataOutput out) throws IOException {
        // Write the data out in the order it is read

        out.writeLong(paragraphCount);
        out.writeDouble(mean);
        out.writeDouble(variance);
    }

    public String toString() {
        return paragraphCount + "," + mean + "," + variance;
    }

}