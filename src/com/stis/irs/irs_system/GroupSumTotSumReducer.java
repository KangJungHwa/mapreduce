
package com.stis.irs.irs_system;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class GroupSumTotSumReducer extends Reducer<Text, Text, Text, Text> {
     private Text outputValue=new Text();
     private Text bKeyText=new Text();
     private Text grTotText=new Text("Group Total");
     long totSum=0;
     long groupSum=0;
     private String bKey="";
     
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
			for (Text val : values) {
				if( key.toString().equals(bKey)) {
					groupSum=groupSum+Long.parseLong(val.toString());
					context.write(key, val);
				}else {
					if(groupSum!= 0) {
					    outputValue.set(String.valueOf(groupSum));
						context.write(grTotText, outputValue);
						groupSum=0;
					 }
						context.write(key, val);

					groupSum=groupSum+Long.parseLong(val.toString());

				}
				bKey=key.toString();
				bKeyText.set(bKey);
				totSum=totSum+Long.parseLong(val.toString());
			}
	}
	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		    outputValue.set(String.valueOf(groupSum));
		    context.write(grTotText, outputValue);
		    
		    outputValue.set(String.valueOf(totSum));
			context.write(new Text("Grand Total"), outputValue);
		}
	}
