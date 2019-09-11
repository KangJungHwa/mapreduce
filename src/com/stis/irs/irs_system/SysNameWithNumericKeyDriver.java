package com.stis.irs.irs_system;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SysNameWithNumericKeyDriver extends Configured implements Tool{
   
	public int run(String[] args) throws Exception{
		String otherArgs[]= new GenericOptionsParser(getConf(), args).getRemainingArgs();
		
		if(otherArgs.length!=2) {
			System.out.println("USAGE # : SysNameWithNumericKeyDriver <IN> <OUT>" );
		}
		
		Job job=new Job(getConf(),"SysNameWithNumericKeyDriver");
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
		job.setJarByClass(SysNameWithNumericKeyDriver.class);
		job.setPartitionerClass(SysNameGroupKeyParitioner.class);
		job.setGroupingComparatorClass(SysNameGroupKeyComparator.class);
		job.setSortComparatorClass(SysNameWithNumericKeyComparator.class);
		
		job.setMapperClass(SysNameWithNumericKeyMapper.class);
		job.setReducerClass(SysNameWithNumericKeyReducer.class);
		
		job.setOutputKeyClass(SysNameWithNumericKey.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.waitForCompletion(true);
		return 0;
	}
	public static void main(String[] args) throws Exception{
		int res=ToolRunner.run(new Configuration(), new SysNameWithNumericKeyDriver(), args);
		System.out.println("RESULT ##:"+res);
	}
}
