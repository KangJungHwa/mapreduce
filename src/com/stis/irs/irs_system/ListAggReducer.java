package com.stis.irs.irs_system;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ListAggReducer extends Reducer<Text, Text, Text, Text> {
     private Text outputValue=new Text();
     private Text bKeyText=new Text();
     String bKey="";
     String appendValue="";

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		for (Text val : values) {
			/*
			 * 키가 같은 경우는 value 문자열을 합한다.
			 **/
			if( key.toString().equals(bKey)) {
				appendValue=appendValue+val.toString()+",";
			}else {
				/*
				 * 마지막 comma는 제거해준다.
				 * 키가 다른 경우는 context write를 한다.
				 * 값을 초기화 한후에 다시 다른경우의 값을 append 해준다..
				 **/
					if(appendValue.length()!= 0) {
					    appendValue=appendValue.substring(0,appendValue.lastIndexOf(","));				
						outputValue.set(appendValue);						
						context.write(bKeyText, outputValue);
						appendValue="";
					}
					appendValue=appendValue+val.toString()+",";
			}
			bKey=key.toString();
			bKeyText.set(bKey);

		}
	}
	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {

			if(appendValue.length()>0) {
				appendValue=appendValue.substring(0,appendValue.lastIndexOf(","));
			}			
			outputValue.set(appendValue);
			context.write(bKeyText, outputValue);
		}
	}
