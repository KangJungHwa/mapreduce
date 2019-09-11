package com.stis.irs.irs_system;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MedianStdDevMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
         private Text outputKey=new Text();
         
         private FloatWritable outValue =new FloatWritable();
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			if(key.get()>0) {
				String columns[]=value.toString().split(",");
				if(columns.length>12) {
				outputKey.set(columns[0]);
				outValue.set(Float.parseFloat(columns[13]));
				
				context.write(outputKey, outValue);
				}
			}
		}
         
         
}
