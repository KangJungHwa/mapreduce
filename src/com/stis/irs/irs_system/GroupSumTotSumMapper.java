package com.stis.irs.irs_system;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class GroupSumTotSumMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Text outputKey= new Text();
    private Text outputValue=new Text();
	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		
		if(key.get()>0) {
         	 String columns[]=value.toString().split(",");
		     if(columns.length>0 && columns!=null) {
		         outputKey.set(columns[2]);
		         outputValue.set(columns[13]);
		         context.write(outputKey, outputValue);
		     }
		}
	}
}
