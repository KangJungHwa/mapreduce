package com.stis.irs.irs_system;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class EqpCodeMapper extends Mapper<LongWritable, Text, Text, Text> {
	// �±� ����
	public final static String DATA_TAG = "A";

	private Text outputKey = new Text();
	private Text outputValue = new Text();

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		if (key.get() > 0) {
			String[] colums = value.toString().replaceAll("\"","").split(",");
			if (colums!=null && colums.length > 0) {// SERVICE TYPE : LTE, cell_num is not null
				outputKey.set(colums[3]+ "_" + DATA_TAG);//SYS_NAME
				outputValue.set(colums[0]+",");
				context.write(outputKey, outputValue);
			}
		}
	}
}
