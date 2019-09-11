package com.stis.irs.irs_system;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class MedianStdDevTuple  implements Writable {
	private float median = 0;
	private float stddev = 0f;
	
	
	public MedianStdDevTuple() {
	}

	public MedianStdDevTuple(float median, float stddev) {
		this.median = median;
		this.stddev = stddev;
	}

	public float getMedian() {
		return median;
	}

	public void setMedian(float median) {
		this.median = median;
	}

	public float getStdDev() {
		return stddev;
	}

	public void setStdDev(float stddev) {
		this.stddev = stddev;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		median = in.readFloat();
		stddev = in.readFloat();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeFloat(median);
		out.writeFloat(stddev);
	}

	@Override
	public String toString() {
		return String.valueOf(median) + "," + String.valueOf(stddev) ;
	}
}

