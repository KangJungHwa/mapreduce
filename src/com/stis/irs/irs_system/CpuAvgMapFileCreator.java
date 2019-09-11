package com.stis.irs.irs_system;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapFileOutputFormat;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class CpuAvgMapFileCreator extends Configured implements Tool {
	@Override
	public int run(String[] args) throws Exception {
		JobConf conf=new JobConf(CpuAvgMapFileCreator.class);
		conf.setJobName("CpuAvgMapFileCreator");
		/*
		  SequenceFileInputFormat
		 */
		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(MapFileOutputFormat.class);
		conf.setOutputKeyClass(IntWritable.class);
		
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		SequenceFileOutputFormat.setCompressOutput(conf, true);
		SequenceFileOutputFormat.setOutputCompressorClass(conf, GzipCodec.class);
		SequenceFileOutputFormat.setOutputCompressionType(conf, CompressionType.BLOCK);
		
		JobClient.runJob(conf);
		return 0;
	}
	public static void main(String[] args) throws Exception{
		int res= ToolRunner.run(new Configuration(), new CpuAvgMapFileCreator(), args);
	  System.out.println("## RESULT : "+res);	
	}
}
