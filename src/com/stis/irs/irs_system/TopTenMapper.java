package com.stis.irs.irs_system;

import java.io.IOException;
import java.util.HashMap;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class TopTenMapper extends Mapper<LongWritable, Text, Text, Text> {
    private TreeMap<Integer, Text> repToRecordMap=new TreeMap<Integer,Text>(); 
    private String sysname=null;

	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
         String columns[]=value.toString().split(",");
         sysname=columns[2];
         repToRecordMap.put(Integer.parseInt(sysname), new Text(value));

    if (repToRecordMap.size() > 3) {
		repToRecordMap.remove(repToRecordMap.firstKey());
	}
	}
}
