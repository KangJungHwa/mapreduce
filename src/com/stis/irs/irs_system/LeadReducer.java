package com.stis.irs.irs_system;


import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LeadReducer extends Reducer<Text, IntWritable,  Text, Text>{
    private Text outputValue=new Text();
	String bCpuAvg="";
	String cpuavg="";
	String bKey="";
	private Text boutputKey=new Text();
	private Text lastoutputKey=new Text();
	int i=0;
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,Context context)
			throws IOException, InterruptedException {
       
		
		for (IntWritable val : values) {
			lastoutputKey.set(key.toString());
			cpuavg=val.toString();
			outputValue.set(bCpuAvg+"#"+cpuavg);
			if(i!=0) {
			context.write(boutputKey, outputValue);
			}
			bCpuAvg=val.toString();
			boutputKey.set(key.toString());
			i++;
		}
	}
	@Override
	/*
	 * map, Reduce 메소드에서 마지막 라인에 대한 작업을 해줄때는 CleanUp 메소드에서 해주면 된다.
	 * @see org.apache.hadoop.mapreduce.Reducer#cleanup(org.apache.hadoop.mapreduce.Reducer.Context)
	 */
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		outputValue.set(cpuavg+"#"+"");
		context.write(lastoutputKey, outputValue);
		super.cleanup(context);
	}
	
}