package com.stis.irs.irs_system;

import java.io.IOException;
import java.util.TreeMap;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class TopTenReducer extends Reducer<Text, Text, Text, Text> {
	
	private TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();

	@Override
	public void reduce(Text key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {
		for (Text value : values) {
           String columns[]=value.toString().split(",");
           if(columns.length>11 && columns!=null){  
				repToRecordMap.put(Integer.parseInt(columns[11]),
						new Text(value));
	
				if (repToRecordMap.size() > 3) {
					repToRecordMap.remove(repToRecordMap.firstKey());
				}
           }
		}

		for (Text t : repToRecordMap.descendingMap().values()) {
			context.write(key, t);
		}
	}
}
