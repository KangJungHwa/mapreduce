package com.stis.irs.irs_system;


import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LagReducer extends Reducer<Text, IntWritable, Text, Text>{
    private Text outputValue=new Text();
	String bCpuAvg="";
	String cpuavg="";
	int cnt=0;
	int cpu=0;
	float avg=0;
	@Override
	protected void reduce(Text key, Iterable<IntWritable> value,Context context)
			throws IOException, InterruptedException {
       /*
		for (IntWritable val : value) {
			cpuavg=val.toString();
			outputValue.set(bCpuAvg+"@"+cpuavg);
			context.write(key, outputValue);
			bCpuAvg=val.toString();
		}*/
		for (IntWritable val : value) {
			cpu+=val.get();
			cnt++;
		}
		avg=cpu/cnt;
		context.write(key, new Text(String.valueOf(avg)));
	}
}