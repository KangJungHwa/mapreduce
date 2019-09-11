package com.stis.irs.irs_system;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;



public class EqpUsageReducer extends Reducer<Text, Text, Text, Text> {

	private Text outputKey = new Text();
	private Text outputValue = new Text();
	String akey="";
	String bkey="";
    public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		String tagValue = key.toString().split("_")[1];

		for (Text value : values) {
			if (tagValue.equals(EqpCodeMapper.DATA_TAG)) {
				akey = key.toString().split("_")[0];				
				outputKey.set(value);
			} else if (tagValue.equals(EqpUsageMapper.DATA_TAG)) {

				bkey = value.toString().split(",")[1];
				if(akey.equals(bkey)) {
					outputValue.set(value);
					context.write(outputKey, outputValue);
				}
				/*
				  else if(akey.equals(bkey)) {
					outputValue.set("");
					outputValue.set(value);
					context.write(outputKey, outputValue);
					
				}*/
				
			}
		}
	}
}
